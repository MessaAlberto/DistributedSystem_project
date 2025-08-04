package it.unitn;

import static org.junit.Assert.*;

import org.junit.Test;

import app.src.main.java.it.unitn.MarsStorageSystem;

public class MarsStorageSystemTest {
    @Test
    public void appHasAGreeting() {
        MarsStorageSystem classUnderTest = new MarsStorageSystem();
        assertNotNull("app should have a greeting", classUnderTest.getGreeting());
    }
}
