package com.assignment.accountservice.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class Constants {
	
	private Constants() {
		//private constructor to hide explicit implementation
	}
    
	public static final String VERSION = "/v1";
	
	public static final String CREATE_ACCOUNT_URL = "/create-account";

	public static final String GET_ACCOUNT_URL = "/get-account/{accountId}";

	public static final String CREATE_TRANSACTION_URL = "/create-transaction";

	public static final String GET_TRANSACTION_URL = "/get-transaction/{accountId}";
	
	public static final List<String> ACCEPTED_CURRENCIES = Collections.unmodifiableList(Arrays.asList("EUR","SEK","GBP","USD"));
	
	public static final String IN = "IN";
	
	public static final String OUT = "OUT";
	
	public static final List<String> ACCEPTED_TRANSACTION_DIRECTION = Collections.unmodifiableList(Arrays.asList(IN,OUT));

	public static final String INVALID_CURRENCY = "Invalid Currency";
	
	public static final Double ZERO_DOUBLE = 0D;

	public static final String CURRENCY_EXCHANGE_RATE_CACHE = "currency_exchange_rate_cache";

	public static final String ACCOUNT_NOT_FOUND = "Account not found";

	public static final String ACCOUNT_MISSING = "Account Missing";

	public static final String INVALID_TRANSACTION_DIRECTION = "Invalid Transaction Direction";

	public static final String INVALID_AMOUNT = "Invalid Transaction Amount";

	public static final String DESCRIPTION_MISSING = "Transaction description missing";

	public static final String INSUFFICIENT_FUNDS = "Insufficient funds for transaction";

	public static final Double ONE_DOUBLE = 1.0;

	public static final Double HUNDRED_DOUBLE = 100.0;

	public static final String ACCOUNT_STATE_CHANGE_QUEUE = "account-state-change-queue";

	public static final String TOPIC_EXCHANGE = "transaction-exchange";

	public static final String ROUTING_KEY = "transation-routing-queue";
	

}
