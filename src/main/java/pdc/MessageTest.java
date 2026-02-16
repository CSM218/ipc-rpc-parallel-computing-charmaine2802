package pdc;

public class MessageTest {
    public static void main(String[] args) {
        // Create a message
        Message original = new Message();
        original.magic = "CSM218";
        original.version = 1;
        original.type = "TEST";
        original.sender = "tester";
        original.timestamp = System.currentTimeMillis();
        original.payload = "Hello World".getBytes();

        // Pack it
        byte[] packed = original.pack();
        System.out.println("✅ Packed message: " + packed.length + " bytes");

        // Unpack it
        Message unpacked = Message.unpack(packed);
        System.out.println("✅ Unpacked magic: " + unpacked.magic);
        System.out.println("✅ Unpacked version: " + unpacked.version);
        System.out.println("✅ Unpacked type: " + unpacked.type);
        System.out.println("✅ Unpacked sender: " + unpacked.sender);
        System.out.println("✅ Unpacked payload: " + new String(unpacked.payload));

        // Verify everything matches
        if (original.magic.equals(unpacked.magic) &&
            original.version == unpacked.version &&
            original.type.equals(unpacked.type) &&
            original.sender.equals(unpacked.sender) &&
            new String(original.payload).equals(new String(unpacked.payload))) {
            System.out.println("\n🎉 SUCCESS! Message.java is working!");
        } else {
            System.out.println("\n❌ FAILED! Something doesn't match.");
        }
    }
}
