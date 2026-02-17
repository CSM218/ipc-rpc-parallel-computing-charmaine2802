package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: Custom wire format with all required protocol fields.
 */
public class Message {
    public String magic;
    public int version;
    public String messageType;   
    public String studentId;        
    public String sender;          
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            
            dos.writeUTF(magic != null ? magic : "CSM218");
            dos.writeInt(version);
            dos.writeUTF(messageType != null ? messageType : "");
            dos.writeUTF(studentId != null ? studentId : ""); 
            dos.writeUTF(sender != null ? sender : "");
            dos.writeLong(timestamp);

            
            if (payload != null) {
                dos.writeInt(payload.length);
                dos.write(payload);
            } else {
                dos.writeInt(0);
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
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            Message msg = new Message();

            
            msg.magic = dis.readUTF();
            msg.version = dis.readInt();
            msg.messageType = dis.readUTF();
            msg.studentId = dis.readUTF();      
            msg.sender = dis.readUTF();
            msg.timestamp = dis.readLong();

            
            int payloadLength = dis.readInt();
            if (payloadLength > 0) {
                msg.payload = new byte[payloadLength];
                dis.readFully(msg.payload);
            } else {
                msg.payload = new byte[0];
            }

            dis.close();
            bais.close();

            return msg;

        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }
}