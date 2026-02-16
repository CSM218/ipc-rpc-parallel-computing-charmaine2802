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
        

            //  Write magic string 
            dos.writeUTF(magic != null ? magic : "");

            //  Write version number
            dos.writeInt(version);

            //  Write message type
            dos.writeUTF(type != null ? type : "");

            //  Write sender ID
            dos.writeUTF(sender != null ? sender : "");

            //  Write timestamp
            dos.writeLong(timestamp);

            //  Write payload length 
            if (payload != null) {
                dos.writeInt(payload.length);
                dos.write(payload); //  Write actual payload data
            } else {
                dos.writeInt(0); // No payload
            }

            dos.flush();
            byte[] result = baos.toByteArray();
            dos.close();
            baos.close();

            return result;

        } catch (IOException e) {
            
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

           

            //  Read magic string
            msg.magic = dis.readUTF();

            //  Read version number
            msg.version = dis.readInt();

            //  Read message type
            msg.type = dis.readUTF();

            //  Read sender ID
            msg.sender = dis.readUTF();

            //  Read timestamp
            msg.timestamp = dis.readLong();

            //  Read payload length
            int payloadLength = dis.readInt();

            //  Read payload data (only if there is any)
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
          
            throw new RuntimeException("Failed to unpack message", e);
        }
    }
}