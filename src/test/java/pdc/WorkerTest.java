package pdc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Disabled;

/**
 * JUnit 5 tests for the Worker class.
 * Tests worker lifecycle and asynchronous behaviors.
 */
class WorkerTest {

    private Worker worker;

    @BeforeEach
    void setUp() {
        worker = new Worker();
    }

   @Test
@Disabled("May block on network timeout - tested by integration tests")
void testWorker_Join_Logic() {
    assertDoesNotThrow(() -> {
        worker.joinCluster("localhost", 9999);
    }, "Worker join logic should handle network absence gracefully");
}

    @Test
    void testWorker_Execute_Invocation() {
        assertDoesNotThrow(() -> {
            worker.execute();
        }, "Worker execute should be a non-blocking invocation of the processing loop");
    }
}
