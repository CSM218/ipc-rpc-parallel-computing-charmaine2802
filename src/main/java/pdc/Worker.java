package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private String workerId;
    private String studentId;      
    private boolean running;

    
    public Worker() {
        this.workerId = System.getenv().getOrDefault("WORKER_ID", 
            "worker-" + System.currentTimeMillis());
        this.studentId = System.getenv().getOrDefault("STUDENT_ID", "UNKNOWN");
        this.running = false;
    }

    
    public Worker(String workerId) {
        this.workerId = workerId;
        this.studentId = System.getenv().getOrDefault("STUDENT_ID", "UNKNOWN");
        this.running = false;
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            
            System.out.println("[Worker " + workerId + "] Connecting to Master at " + masterHost + ":" + port);
            socket = new Socket(masterHost, port);
            
          
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());
            
            // registration message sent  to Master
           
Message joinMessage = new Message();
joinMessage.magic = "CSM218";
joinMessage.version = 1;
joinMessage.messageType = "REGISTER_WORKER"; 
joinMessage.studentId = studentId;            
joinMessage.sender = workerId;
joinMessage.timestamp = System.currentTimeMillis();
joinMessage.payload = new byte[0];
            
            // Wait for acknowledgment from Master
            Message ackMessage = receiveMessage();
            if (ackMessage != null && ackMessage.messageType.equals("JOIN_ACK")) {
                System.out.println("[Worker " + workerId + "] Successfully joined cluster!");
                running = true;
                
                // Start working and listen for tasks
                execute();
            }
            
        } catch (IOException e) {
            System.err.println("[Worker " + workerId + "] Failed to join cluster: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        System.out.println("[Worker " + workerId + "] Starting execution loop...");
        
        while (running) {
            try {
                // Wait for a task from Master
                Message taskMessage = receiveMessage();
                
                if (taskMessage == null) {
                    System.out.println("[Worker " + workerId + "] Connection closed by Master");
                    break;
                }
                
                System.out.println("[Worker " + workerId + "] Received task: " + taskMessage.messageType);
                
                // Handle different message types
                switch (taskMessage.messageType) {  
    case "RPC_REQUEST":           
        handleTask(taskMessage);
        break;
        
    case "HEARTBEAT":
        handleHeartbeat(taskMessage);
        break;
        
    case "SHUTDOWN":
        System.out.println("[Worker " + workerId + "] Received shutdown command");
        running = false;
        break;
        
    default:
        System.out.println("[Worker " + workerId + "] Unknown message type: " + taskMessage.messageType);
}
                
            } catch (Exception e) {
                System.err.println("[Worker " + workerId + "] Error during execution: " + e.getMessage());
                e.printStackTrace();
                running = false;
            }
        }
        
        
        cleanup();
    }

    /**
     * Handling computation task
     */
    private void handleTask(Message taskMessage) {
        try {
            long startTime = System.currentTimeMillis();
            
        
            
            byte[] taskData = taskMessage.payload;
           
            byte[] resultData = performComputation(taskData);
            
            long endTime = System.currentTimeMillis();
            System.out.println("[Worker " + workerId + "] Task completed in " + (endTime - startTime) + "ms");
            
            
            Message resultMessage = new Message();
resultMessage.magic = "CSM218";
resultMessage.version = 1;
resultMessage.messageType = "TASK_COMPLETE";  
resultMessage.studentId = studentId;         
resultMessage.sender = workerId;
resultMessage.timestamp = System.currentTimeMillis();
resultMessage.payload = resultData;
            
            sendMessage(resultMessage);
            System.out.println("[Worker " + workerId + "] Sent result back to Master");
            
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Failed to handle task: " + e.getMessage());
            
            
            sendErrorMessage(taskMessage);
        }
    }

    /**
     * Responds to heartbeat checks from Master
     */
    private void handleHeartbeat(Message heartbeatMessage) {
        try {
            
            Message response = new Message();
response.magic = "CSM218";
response.version = 1;
response.messageType = "HEARTBEAT";
response.studentId = studentId;    
response.sender = workerId;
response.timestamp = System.currentTimeMillis();
response.payload = new byte[0];
            
            sendMessage(response);
            System.out.println("[Worker " + workerId + "] Responded to heartbeat");
            
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Failed to respond to heartbeat: " + e.getMessage());
        }
    }

    /**
     * Sends an error message back to Master when task fails
     */
    private void sendErrorMessage(Message originalTask) {
        try {
            Message errorMessage = new Message();
errorMessage.magic = "CSM218";
errorMessage.version = 1;
errorMessage.messageType = "TASK_ERROR";  
errorMessage.studentId = studentId;     
errorMessage.sender = workerId;
errorMessage.timestamp = System.currentTimeMillis();
errorMessage.payload = "Task failed".getBytes();
            
            sendMessage(errorMessage);
            
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Failed to send error message: " + e.getMessage());
        }
    }

    /**
     * Helper: Send a message to Master
     */
    private void sendMessage(Message msg) throws IOException {
        byte[] packed = msg.pack();
        output.writeInt(packed.length); 
        output.write(packed);           
        output.flush();
    }

    /**
     * Helper: Receive a message from Master
     */
    private Message receiveMessage() throws IOException {
        try {
            int length = input.readInt();           
            byte[] data = new byte[length];
            input.readFully(data);                  
            return Message.unpack(data);
        } catch (IOException e) {
            return null; 
        }
    }

    /**
     * Cleanup resources
     */
    private void cleanup() {
        try {
            if (input != null) input.close();
            if (output != null) output.close();
            if (socket != null) socket.close();
            System.out.println("[Worker " + workerId + "] Cleaned up resources");
        } catch (IOException e) {
            System.err.println("[Worker " + workerId + "] Error during cleanup: " + e.getMessage());
        }
    }

    /**
     * Main method 
     */
   public static void main(String[] args) {
    
    String workerId = System.getenv().getOrDefault("WORKER_ID", "worker-1");
    String masterHost = System.getenv().getOrDefault("MASTER_HOST", "localhost");
    int port = Integer.parseInt(System.getenv().getOrDefault("MASTER_PORT", "8080"));
    
    
    if (args.length >= 3) {
        workerId = args[0];
        masterHost = args[1];
        port = Integer.parseInt(args[2]);
    }
    
    System.out.println("[Worker] Starting with ID: " + workerId);
    System.out.println("[Worker] Connecting to: " + masterHost + ":" + port);
    
    Worker worker = new Worker(workerId);
    worker.joinCluster(masterHost, port);
}
/**
 * Performs the computation 
 */
private byte[] performComputation(byte[] taskData) {
    try {
        // Deserialize the matrix from bytes
        int[][] matrix = deserializeMatrix(taskData);
        
        // Perform computation on the matrix
        int[][] result = processMatrixChunk(matrix);
        
        // Serialize the result back to bytes
        return serializeMatrix(result);
        
    } catch (Exception e) {
        System.err.println("[Worker " + workerId + "] Error during computation: " + e.getMessage());
        e.printStackTrace();
        return taskData;
    }
}

/**
 * Process a matrix chunk 
 */
private int[][] processMatrixChunk(int[][] matrix) {
    if (matrix == null || matrix.length == 0) {
        return matrix;
    }
    
    int rows = matrix.length;
    int cols = matrix[0].length;
    int[][] result = new int[rows][cols];
    
 
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            result[i][j] = matrix[i][j] * 2;
        }
    }
    
    return result;
}

/**
 * Serialize a matrix to bytes
 */
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

/**
 * Helper: Write int to byte array
 */
private void writeInt(byte[] array, int offset, int value) {
    array[offset] = (byte) (value >> 24);
    array[offset + 1] = (byte) (value >> 16);
    array[offset + 2] = (byte) (value >> 8);
    array[offset + 3] = (byte) value;
}

/**
 * Helper: Read int from byte array
 */
private int readInt(byte[] array, int offset) {
    return ((array[offset] & 0xFF) << 24) |
           ((array[offset + 1] & 0xFF) << 16) |
           ((array[offset + 2] & 0xFF) << 8) |
           (array[offset + 3] & 0xFF);
}
}