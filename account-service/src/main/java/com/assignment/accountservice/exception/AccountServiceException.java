package com.assignment.accountservice.exception;

import com.assignment.accountservice.util.ErrorCode;

public class AccountServiceException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8906345755378013193L;

	private ErrorCode errorCode;
	
	private String message;

	public ErrorCode getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(ErrorCode errorCode) {
		this.errorCode = errorCode;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public AccountServiceException(ErrorCode errorCode, String message) {
		super();
		this.errorCode = errorCode;
		this.message = message;
	}

	public AccountServiceException(ErrorCode errorCode) {
		this.errorCode = errorCode;
	}
  
}
