package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        try {
            // Create a stream to write bytes to
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // Write each field in a specific order
            // The receiver will read in the SAME order

            // 1. Write magic string (protocol identifier)
            dos.writeUTF(magic != null ? magic : "");

            // 2. Write version number
            dos.writeInt(version);

            // 3. Write message type
            dos.writeUTF(type != null ? type : "");

            // 4. Write sender ID
            dos.writeUTF(sender != null ? sender : "");

            // 5. Write timestamp
            dos.writeLong(timestamp);

            // 6. Write payload length (CRITICAL: so receiver knows how much to read)
            if (payload != null) {
                dos.writeInt(payload.length);
                dos.write(payload); // 7. Write actual payload data
            } else {
                dos.writeInt(0); // No payload
            }

            // Close streams and return the byte array
            dos.flush();
            byte[] result = baos.toByteArray();
            dos.close();
            baos.close();

            return result;

        } catch (IOException e) {
            // If something goes wrong, wrap it in a runtime exception
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            // Create a stream to read bytes from
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            // Create a new message to fill
            Message msg = new Message();

            // Read each field in the SAME order we wrote them in pack()

            // 1. Read magic string
            msg.magic = dis.readUTF();

            // 2. Read version number
            msg.version = dis.readInt();

            // 3. Read message type
            msg.type = dis.readUTF();

            // 4. Read sender ID
            msg.sender = dis.readUTF();

            // 5. Read timestamp
            msg.timestamp = dis.readLong();

            // 6. Read payload length
            int payloadLength = dis.readInt();

            // 7. Read payload data (only if there is any)
            if (payloadLength > 0) {
                msg.payload = new byte[payloadLength];
                dis.readFully(msg.payload); // Read exactly payloadLength bytes
            } else {
                msg.payload = new byte[0]; // Empty payload
            }

            // Close streams and return the message
            dis.close();
            bais.close();

            return msg;

        } catch (IOException e) {
            // If something goes wrong, wrap it in a runtime exception
            throw new RuntimeException("Failed to unpack message", e);
        }
    }
}