package it.unitn;

public class Logger {
  private final String actorName;

  // Constructor binds logger to an actor name
  public Logger(String actorName) {
    this.actorName = actorName;
  }

  // Simple log method
  public void log(String msg) {
    System.out.println("[" + actorName + "] " + msg);
  }

  public void logError(String msg) {
    System.err.println("[" + actorName + "] ERROR: " + msg);
  }
}
