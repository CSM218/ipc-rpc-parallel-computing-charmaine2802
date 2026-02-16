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
    private boolean running;

    // Default constructor
    public Worker() {
        this.workerId = "worker-" + System.currentTimeMillis();
        this.running = false;
    }

    // Constructor with ID
    public Worker(String workerId) {
        this.workerId = workerId;
        this.running = false;
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            // connection to the master
            System.out.println("[Worker " + workerId + "] Connecting to Master at " + masterHost + ":" + port);
            socket = new Socket(masterHost, port);
            
            // Set up for input/output streams
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());
            
            // registration message sent  to Master
            Message joinMessage = new Message();
            joinMessage.magic = "CSM218";
            joinMessage.version = 1;
            joinMessage.type = "WORKER_JOIN";
            joinMessage.sender = workerId;
            joinMessage.timestamp = System.currentTimeMillis();
            joinMessage.payload = new byte[0]; // Empty payload for join
            
            sendMessage(joinMessage);
            System.out.println("[Worker " + workerId + "] Sent JOIN request to Master");
            
            // Wait for acknowledgment from Master
            Message ackMessage = receiveMessage();
            if (ackMessage != null && ackMessage.type.equals("JOIN_ACK")) {
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
                
                System.out.println("[Worker " + workerId + "] Received task: " + taskMessage.type);
                
                // Handle different message types
                switch (taskMessage.type) {
                    case "TASK":
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
                        System.out.println("[Worker " + workerId + "] Unknown message type: " + taskMessage.type);
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
     * Handles an actual computation task
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
            resultMessage.type = "RESULT";
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
     * Performs the actual computation (matrix multiplication or other operations)
     */
    private byte[] performComputation(byte[] taskData) {
        // TODO: Implement actual matrix multiplication logic here
        // For now, just echo back the data (placeholder)
        
       
        try {
            Thread.sleep(100); // Simulating computation time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return taskData; // Placeholder - replace with actual computation
    }

    /**
     * Responds to heartbeat checks from Master
     */
    private void handleHeartbeat(Message heartbeatMessage) {
        try {
            // Send heartbeat response
            Message response = new Message();
            response.magic = "CSM218";
            response.version = 1;
            response.type = "HEARTBEAT_ACK";
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
            errorMessage.type = "ERROR";
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
        output.writeInt(packed.length); // Send length first
        output.write(packed);           // Then send the actual message
        output.flush();
    }

    /**
     * Helper: Receive a message from Master
     */
    private Message receiveMessage() throws IOException {
        try {
            int length = input.readInt();           // Read length first
            byte[] data = new byte[length];
            input.readFully(data);                  // Read exact number of bytes
            return Message.unpack(data);
        } catch (IOException e) {
            return null; // Connection closed
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
     * Main method for testing (you can run a worker standalone)
     */
    public static void main(String[] args) {
        String workerId = "worker-1";
        String masterHost = "localhost";
        int port = 8080;
        
        // Parse command line arguments if provided
        if (args.length >= 3) {
            workerId = args[0];
            masterHost = args[1];
            port = Integer.parseInt(args[2]);
        }
        
        Worker worker = new Worker(workerId);
        worker.joinCluster(masterHost, port);
    }
}