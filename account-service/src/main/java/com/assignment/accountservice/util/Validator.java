package com.assignment.accountservice.util;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.assignment.accountservice.domain.Account;
import com.assignment.accountservice.exception.AccountServiceException;
import com.assignment.accountservice.model.TransactionRequestModel;

public final class Validator {
	
	private Validator() {
		// private constructor to hide explicit implementation
	}
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Validator.class);
	
	
	public static void validateCurrency(List<String> currencies) throws AccountServiceException {
		boolean isInvalidCurrency = true;		
		if(!CollectionUtils.isEmpty(currencies)) {			
			isInvalidCurrency = currencies.stream().anyMatch(s -> !Constants.ACCEPTED_CURRENCIES.contains(s));
		}		
		if(isInvalidCurrency) {			
			LOGGER.debug("Currency validation failed");
			throw new AccountServiceException(ErrorCode.INVALID_CURRENCY, Constants.INVALID_CURRENCY);
		}
		
	}
	
	public static void staticValidationForTransactionRequest(TransactionRequestModel transactionModel, Account account) throws AccountServiceException {
		
		if(account == null) {
			LOGGER.debug("Account missing for transaction request:: {}", transactionModel.getAccountId());
			throw new AccountServiceException(ErrorCode.ACCOUNT_NOT_FOUND,Constants.ACCOUNT_MISSING);
		}
		if(transactionModel.getAmount() < 0) {
			LOGGER.debug("Invalid transaction amount :: {}",transactionModel.getAmount());
			throw new AccountServiceException(ErrorCode.INVALID_AMOUNT, Constants.INVALID_AMOUNT);
		}
		if(!Constants.ACCEPTED_TRANSACTION_DIRECTION.contains(transactionModel.getDirectionOfTransaction())) {			
			LOGGER.debug("Direction validation failed");
			throw new AccountServiceException(ErrorCode.INVALID_TRANSACTION_DIRECTION, Constants.INVALID_TRANSACTION_DIRECTION);
		}
		if(!StringUtils.hasText(transactionModel.getDescription())) {			
			LOGGER.debug("Description not available");
			throw new AccountServiceException(ErrorCode.DESCRIPTION_MISSING, Constants.DESCRIPTION_MISSING);
		}
		
	}

}
