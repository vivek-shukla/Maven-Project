package com.assignment.accountservice.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import com.assignment.accountservice.exception.AccountServiceException;

@ControllerAdvice
public class AccountControllerAdvice {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AccountControllerAdvice.class);
	
	/*
	 * Handles Custom Validation AccountServiceException 
	 * 
	 * @param AccountServiceException
	 * @return ResponseEntity
	 * 
	 */
	@ExceptionHandler(value = AccountServiceException.class)
	public ResponseEntity<?> customException(AccountServiceException a){	
		LOGGER.error("Custom validation failed :: {}" , a.getMessage());
		return ResponseEntity.status(a.getErrorCode().getErrorStatus()).body(a.getMessage());
		
	}
	
	/*
	 * Handles System Exception
	 * 
	 * @param AccountServiceException
	 * @return ResponseEntity - INTERNAL_SERVER_ERROR
	 * 
	 */
	@ExceptionHandler(value = Exception.class)
	public ResponseEntity<?> customException(Exception e){
		LOGGER.error("System Error Occured :: {}" , e.getMessage());
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getLocalizedMessage());
		
	}

}
