package com.blueskyarea.exception;

public class HadoopJobHistorySaverRuntimeException extends RuntimeException {

	private static final long serialVersionUID = 6075895065505070833L;

	public HadoopJobHistorySaverRuntimeException(String message) {
        super(message);
    }

    public HadoopJobHistorySaverRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
