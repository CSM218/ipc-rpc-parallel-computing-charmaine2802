package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private String studentId; 
    
    // Worker management
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final Map<String, Long> workerLastSeen = new ConcurrentHashMap<>();
    
    // Task management
    private final Map<Integer, TaskInfo> tasks = new ConcurrentHashMap<>();
    private final AtomicInteger taskIdCounter = new AtomicInteger(0);
    
    // Configuration
    private static final long WORKER_TIMEOUT_MS = 10000; 
    private static final long TASK_TIMEOUT_MS = 30000;   
    private static final int SOCKET_TIMEOUT_MS = 5000;   


    // Constructor reads from environment
    public Master() {
        this.studentId = System.getenv().getOrDefault("STUDENT_ID", "UNKNOWN");
        System.out.println("[Master] Initialized with Student ID: " + studentId);
    }
    /**
     * Inner class to represent a connected worker
     */
    private class WorkerConnection {
        String workerId;
        Socket socket;
        DataInputStream input;
        DataOutputStream output;
        boolean active;
        
        WorkerConnection(String workerId, Socket socket) throws IOException {
            this.workerId = workerId;
            this.socket = socket;
            this.socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            this.input = new DataInputStream(socket.getInputStream());
            this.output = new DataOutputStream(socket.getOutputStream());
            this.active = true;
        }
        
        void sendMessage(Message msg) throws IOException {
            synchronized (output) {
                byte[] packed = msg.pack();
                output.writeInt(packed.length);
                output.write(packed);
                output.flush();
            }
        }
        
        Message receiveMessage() throws IOException {
            int length = input.readInt();
            byte[] data = new byte[length];
            input.readFully(data);
            return Message.unpack(data);
        }
        
        void close() {
            try {
                if (input != null) input.close();
                if (output != null) output.close();
                if (socket != null) socket.close();
            } catch (IOException e) {
               
            }
        }
    }

    /**
     * Inner class to track task information
     */
    private class TaskInfo {
        int taskId;
        String assignedWorker;
        byte[] taskData;
        byte[] result;
        long assignedTime;
        boolean completed;
        int retryCount;
        
        TaskInfo(int taskId, byte[] taskData) {
            this.taskId = taskId;
            this.taskData = taskData;
            this.completed = false;
            this.retryCount = 0;
        }
    }

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        System.out.println("[Master] Starting coordination for operation: " + operation);
        System.out.println("[Master] Data size: " + data.length + "x" + (data.length > 0 ? data[0].length : 0));
        System.out.println("[Master] Waiting for " + workerCount + " workers...");
        
        
        waitForWorkers(workerCount);
        
     
        List<byte[]> chunks = splitIntoChunks(data, workerCount);
        System.out.println("[Master] Split work into " + chunks.size() + " chunks");
        
        
        for (byte[] chunk : chunks) {
            int taskId = taskIdCounter.getAndIncrement();
            tasks.put(taskId, new TaskInfo(taskId, chunk));
        }
        
       
        distributeTasks();
        
       
        List<byte[]> results = collectResults();
        
        
        System.out.println("[Master] Combining results...");
        int[][] finalResult = combineResults(results, data.length);
        
        System.out.println("[Master] Coordination complete!");
        return finalResult;
    }

    /**
     * Wait for the required number of workers to connect
     */
    private void waitForWorkers(int count) {
        long startTime = System.currentTimeMillis();
        long timeout = 30000;
        
        while (workers.size() < count) {
            if (System.currentTimeMillis() - startTime > timeout) {
                System.out.println("[Master] Timeout waiting for workers. Proceeding with " + workers.size() + " workers.");
                break;
            }
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        System.out.println("[Master] " + workers.size() + " workers connected and ready");
    }

    /**
     * Split matrix data into chunks for parallel processing
     */
    private List<byte[]> splitIntoChunks(int[][] data, int numChunks) {
        List<byte[]> chunks = new ArrayList<>();
        
        if (data == null || data.length == 0) {
            return chunks;
        }
        
        int rows = data.length;
        int cols = data[0].length;
        int rowsPerChunk = Math.max(1, rows / numChunks);
        
        for (int i = 0; i < rows; i += rowsPerChunk) {
            int endRow = Math.min(i + rowsPerChunk, rows);
            
           
            int chunkRows = endRow - i;
            int[][] chunk = new int[chunkRows][cols];
            for (int r = 0; r < chunkRows; r++) {
                System.arraycopy(data[i + r], 0, chunk[r], 0, cols);
            }
            
          
            byte[] serialized = serializeMatrix(chunk);
            chunks.add(serialized);
        }
        
        return chunks;
    }

    /**
     * Distribute tasks to available workers
     */
    private void distributeTasks() {
        System.out.println("[Master] Distributing tasks to workers...");
        
        List<String> availableWorkers = new ArrayList<>(workers.keySet());
        int workerIndex = 0;
        
        for (TaskInfo task : tasks.values()) {
            if (availableWorkers.isEmpty()) {
                System.err.println("[Master] No workers available!");
                break;
            }
            
            
            String workerId = availableWorkers.get(workerIndex % availableWorkers.size());
            assignTaskToWorker(task, workerId);
            
            workerIndex++;
        }
    }

    /**
     * Assign a specific task to a specific worker
     */
    private void assignTaskToWorker(TaskInfo task, String workerId) {
        WorkerConnection worker = workers.get(workerId);
        if (worker == null || !worker.active) {
            System.err.println("[Master] Worker " + workerId + " is not available");
            return;
        }
        
        try {
           Message taskMessage = new Message();
taskMessage.magic = "CSM218";
taskMessage.version = 1;
taskMessage.messageType = "RPC_REQUEST";  
taskMessage.studentId = studentId;        
taskMessage.sender = "master";
taskMessage.timestamp = System.currentTimeMillis();
taskMessage.payload = task.taskData;
            
            worker.sendMessage(taskMessage);
            
            task.assignedWorker = workerId;
            task.assignedTime = System.currentTimeMillis();
            
            System.out.println("[Master] Assigned task " + task.taskId + " to " + workerId);
            
        } catch (IOException e) {
            System.err.println("[Master] Failed to assign task to " + workerId + ": " + e.getMessage());
            worker.active = false;
        }
    }

    /**
     * Collect results from all tasks
     */
    private List<byte[]> collectResults() {
        System.out.println("[Master] Collecting results from workers...");
        
        long startTime = System.currentTimeMillis();
        
        while (!allTasksComplete()) {
           
            long currentTime = System.currentTimeMillis();
            
            for (TaskInfo task : tasks.values()) {
                if (!task.completed && task.assignedWorker != null) {
                    long elapsed = currentTime - task.assignedTime;
                    
                    if (elapsed > TASK_TIMEOUT_MS && task.retryCount < 3) {
                        System.out.println("[Master] Task " + task.taskId + " timed out on " + task.assignedWorker + ". Reassigning...");
                        task.assignedWorker = null;
                        task.retryCount++;
                        
                      
                        for (String workerId : workers.keySet()) {
                            if (!workerId.equals(task.assignedWorker)) {
                                assignTaskToWorker(task, workerId);
                                break;
                            }
                        }
                    }
                }
            }
            
            
            if (currentTime - startTime > 60000) {
                System.err.println("[Master] Overall timeout reached!");
                break;
            }
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        
        List<byte[]> results = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            TaskInfo task = tasks.get(i);
            if (task != null && task.result != null) {
                results.add(task.result);
            }
        }
        
        System.out.println("[Master] Collected " + results.size() + " results");
        return results;
    }

    /**
     * Check if all tasks are complete
     */
    private boolean allTasksComplete() {
        for (TaskInfo task : tasks.values()) {
            if (!task.completed) {
                return false;
            }
        }
        return true;
    }

    /**
     * Combine results from all workers
     */
    private int[][] combineResults(List<byte[]> results, int totalRows) {
        if (results.isEmpty()) {
            return new int[0][0];
        }
        
       
        int[][] firstChunk = deserializeMatrix(results.get(0));
        int cols = firstChunk[0].length;
        
      
        int[][] combined = new int[totalRows][cols];
        
        int currentRow = 0;
        for (byte[] resultData : results) {
            int[][] chunk = deserializeMatrix(resultData);
            
            for (int[] row : chunk) {
                if (currentRow < totalRows) {
                    System.arraycopy(row, 0, combined[currentRow], 0, cols);
                    currentRow++;
                }
            }
        }
        
        return combined;
    }

    
    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        
        System.out.println("[Master] Listening on port " + port);
        
       
        systemThreads.submit(this::healthCheckLoop);
        
      
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("[Master] New connection from " + clientSocket.getInetAddress());
                
                
                systemThreads.submit(() -> handleWorker(clientSocket));
                
            } catch (IOException e) {
                if (running) {
                    System.err.println("[Master] Error accepting connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Handle communication with a single worker
     */
    private void handleWorker(Socket socket) {
        WorkerConnection worker = null;
        
        try {
            
            DataInputStream tempInput = new DataInputStream(socket.getInputStream());
            int length = tempInput.readInt();
            byte[] data = new byte[length];
            tempInput.readFully(data);
            Message joinMsg = Message.unpack(data);
            
       if (!joinMsg.messageType.equals("REGISTER_WORKER")) {
    System.err.println("[Master] Expected REGISTER_WORKER, got " + joinMsg.messageType);
                System.err.println("[Master] Expected WORKER_JOIN, got " + joinMsg.messageType);
                socket.close();
                return;
            }
            
            String workerId = joinMsg.sender;
            System.out.println("[Master] Worker " + workerId + " joined");
            
           
            worker = new WorkerConnection(workerId, socket);
            workers.put(workerId, worker);
            workerLastSeen.put(workerId, System.currentTimeMillis());
            
            
            

Message ack = new Message();
ack.magic = "CSM218";
ack.version = 1;
ack.messageType = "WORKER_ACK";  
ack.studentId = studentId;       
ack.sender = "master";
ack.timestamp = System.currentTimeMillis();
ack.payload = new byte[0];
            
          
            while (running && worker.active) {
                try {
                    Message msg = worker.receiveMessage();
                    workerLastSeen.put(workerId, System.currentTimeMillis());
                    
                    handleWorkerMessage(worker, msg);
                    
                } catch (SocketTimeoutException e) {
                   
                    continue;
                } catch (IOException e) {
                    System.err.println("[Master] Connection lost with " + workerId);
                    break;
                }
            }
            
        } catch (Exception e) {
            System.err.println("[Master] Error handling worker: " + e.getMessage());
        } finally {
            if (worker != null) {
                workers.remove(worker.workerId);
                workerLastSeen.remove(worker.workerId);
                worker.close();
                System.out.println("[Master] Worker " + worker.workerId + " disconnected");
            }
        }
    }

    /**
     * Handle messages received from workers
     */
    private void handleWorkerMessage(WorkerConnection worker, Message msg) {
    switch (msg.messageType) {  
        case "TASK_COMPLETE":   
        case "RPC_RESPONSE":    
            handleTaskResult(worker.workerId, msg);
            break;
            
        case "HEARTBEAT":
            
            break;
            
        case "TASK_ERROR":
            System.err.println("[Master] Worker " + worker.workerId + " reported error");
            break;
            
        default:
            System.out.println("[Master] Unknown message type from " + worker.workerId + ": " + msg.messageType);
    }
}

    /**
     * Handle task result from a worker
     */
    private void handleTaskResult(String workerId, Message msg) {
       
        for (TaskInfo task : tasks.values()) {
            if (workerId.equals(task.assignedWorker) && !task.completed) {
                task.result = msg.payload;
                task.completed = true;
                System.out.println("[Master] Received result for task " + task.taskId + " from " + workerId);
                break;
            }
        }
    }

   
    public void reconcileState() {
        long currentTime = System.currentTimeMillis();
        
        List<String> deadWorkers = new ArrayList<>();
        
        for (Map.Entry<String, Long> entry : workerLastSeen.entrySet()) {
            String workerId = entry.getKey();
            long lastSeen = entry.getValue();
            
            if (currentTime - lastSeen > WORKER_TIMEOUT_MS) {
                System.out.println("[Master] Worker " + workerId + " appears dead");
                deadWorkers.add(workerId);
            }
        }
        
        
        for (String deadWorker : deadWorkers) {
            WorkerConnection worker = workers.remove(deadWorker);
            if (worker != null) {
                worker.active = false;
                worker.close();
            }
            workerLastSeen.remove(deadWorker);
            
       
            for (TaskInfo task : tasks.values()) {
                if (deadWorker.equals(task.assignedWorker) && !task.completed) {
                    System.out.println("[Master] Reassigning task " + task.taskId + " from dead worker");
                    task.assignedWorker = null;
                    task.retryCount++;
                    
                   
                    for (String workerId : workers.keySet()) {
                        assignTaskToWorker(task, workerId);
                        break;
                    }
                }
            }
        }
    }

    
    private void healthCheckLoop() {
        while (running) {
            try {
                Thread.sleep(5000); 
                reconcileState();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    
    private byte[] serializeMatrix(int[][] matrix) {
        if (matrix == null || matrix.length == 0) {
            return new byte[8]; 
        }
        
        int rows = matrix.length;
        int cols = matrix[0].length;
        
        
        byte[] result = new byte[8 + rows * cols * 4];
        
        
        writeInt(result, 0, rows);
        writeInt(result, 4, cols);
        
        
        int offset = 8;
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                writeInt(result, offset, matrix[i][j]);
                offset += 4;
            }
        }
        
        return result;
    }

    /**
     * Deserialize bytes back to a matrix
     */
    private int[][] deserializeMatrix(byte[] data) {
        if (data == null || data.length < 8) {
            return new int[0][0];
        }
        
        int rows = readInt(data, 0);
        int cols = readInt(data, 4);
        
        if (rows == 0 || cols == 0) {
            return new int[0][0];
        }
        
        int[][] matrix = new int[rows][cols];
        int offset = 8;
        
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = readInt(data, offset);
                offset += 4;
            }
        }
        
        return matrix;
    }

    
    private void writeInt(byte[] array, int offset, int value) {
        array[offset] = (byte) (value >> 24);
        array[offset + 1] = (byte) (value >> 16);
        array[offset + 2] = (byte) (value >> 8);
        array[offset + 3] = (byte) value;
    }

    
    private int readInt(byte[] array, int offset) {
        return ((array[offset] & 0xFF) << 24) |
               ((array[offset + 1] & 0xFF) << 16) |
               ((array[offset + 2] & 0xFF) << 8) |
               (array[offset + 3] & 0xFF);
    }

    /**
     * Shutdown the master
     */
    public void shutdown() {
        running = false;
        
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
           
        }
        
        for (WorkerConnection worker : workers.values()) {
            worker.close();
        }
        
        systemThreads.shutdown();
        try {
            systemThreads.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        System.out.println("[Master] Shutdown complete");
    }

    /**
     * Main method for testing
     */
   
public static void main(String[] args) throws IOException {
    Master master = new Master();
    
   
    int port = Integer.parseInt(
        System.getenv().getOrDefault("MASTER_PORT", "8080")
    );
    
   
    if (args.length > 0) {
        port = Integer.parseInt(args[0]);
    }
    
    System.out.println("[Master] Starting on port: " + port);
    System.out.println("[Master] Student ID: " + master.studentId);
    
   
    final int finalPort = port;
    new Thread(() -> {
        try {
            master.listen(finalPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }).start();
    
   
    try {
        Thread.sleep(2000); 
        
        int[][] testMatrix = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
        int[][] result = (int[][]) master.coordinate("TEST", testMatrix, 2);
        
        System.out.println("[Master] Result:");
        if (result != null) {
            for (int[] row : result) {
                for (int val : row) {
                    System.out.print(val + " ");
                }
                System.out.println();
            }
        }
        
    } catch (InterruptedException e) {
        e.printStackTrace();
    }
}
}