package com.blueskyarea.exception;

public class HadoopJobHistorySaverException extends Exception {

	private static final long serialVersionUID = 7186582757447684133L;

    public HadoopJobHistorySaverException(String message) {
        super(message);
    }

    public HadoopJobHistorySaverException(String message, Throwable cause) {
        super(message, cause);
    }
}
