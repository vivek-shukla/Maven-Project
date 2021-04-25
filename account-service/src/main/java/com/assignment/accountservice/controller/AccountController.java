package com.assignment.accountservice.controller;

import java.util.List;

import javax.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.assignment.accountservice.exception.AccountServiceException;
import com.assignment.accountservice.model.AccountRequestModel;
import com.assignment.accountservice.model.AccountResponseModel;
import com.assignment.accountservice.model.TransactionRequestModel;
import com.assignment.accountservice.model.TransactionResponseModel;
import com.assignment.accountservice.service.AccountService;
import com.assignment.accountservice.util.Constants;

@RestController(value = Constants.VERSION)
public class AccountController {
	
	@Autowired
	private AccountService accountService;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AccountController.class);
    
	@PostMapping(path = Constants.CREATE_ACCOUNT_URL,consumes = MediaType.APPLICATION_JSON_VALUE, 
			produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<AccountResponseModel> createAccount(@RequestBody(required = true) @Valid AccountRequestModel accountDTO) throws AccountServiceException{
		
		LOGGER.debug("Account Controller :: Create Account Invoked");
		return ResponseEntity.ok(this.accountService.createAccount(accountDTO));
	}
	
	@GetMapping(path = Constants.GET_ACCOUNT_URL,produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<AccountResponseModel> fetchByAccountId(@PathVariable(required = true,name = "accountId") Long accountId) 
			throws AccountServiceException{		
		LOGGER.debug("Account Controller :: Get Acccount Invoked");
		return ResponseEntity.ok(this.accountService.fetchByAccountId(accountId));
	}
	
	@PostMapping(path = Constants.CREATE_TRANSACTION_URL,consumes = MediaType.APPLICATION_JSON_VALUE, 
			produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<TransactionResponseModel> createTransaction(@RequestBody(required = true) @Valid TransactionRequestModel transactionModel) throws AccountServiceException{
		
		LOGGER.debug("Account Controller :: Create Transaction Invoked");
		return ResponseEntity.ok(this.accountService.createTransaction(transactionModel));
	}
	
	@GetMapping(path = Constants.GET_TRANSACTION_URL,produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<TransactionResponseModel>> fetchTransactionByAccountId(@PathVariable(required = true,name = "accountId") Long accountId) 
			throws AccountServiceException{
		
		LOGGER.debug("Account Controller :: Get Transaction Invoked");
		return ResponseEntity.ok(this.accountService.fetchTransactionByAccountId(accountId));
	}
		
}
