package com.blueskyarea.exception;

public class HadoopResultSaverException extends Exception {

	private static final long serialVersionUID = 7186582757447684133L;

    public HadoopResultSaverException(String message) {
        super(message);
    }

    public HadoopResultSaverException(String message, Throwable cause) {
        super(message, cause);
    }
}
