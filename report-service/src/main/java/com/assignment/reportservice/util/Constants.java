package com.assignment.reportservice.util;

public final class Constants {
	
	private Constants() {
		//public static final constructor to hide explicit implementation
	}
	
	public static final String ACCOUNT_STATE_CHANGE_QUEUE = "account-state-change-queue";

	public static final String TOPIC_EXCHANGE = "transaction-exchange";

	public static final String ROUTING_KEY = "transation-routing-queue";

	public static final String BUCKET = "vivekshukla.av's Bucket";

	public static final String ORG = "vivekshukla.av@gmail.com";

	public static final String MEASUREMENT = "Account-State-Change";
	
	public static final String STATE_CHANGE_TAG = "STATE_CHANGE_TYPE";	
	
    public static final String CUSTOMER_ID = "customerId";
	
	public static final String ACCOUNT_ID = "accountId";
	
	public static final String COUNTRY = "country";
	
	public static final String BALANCE = "balance";
	
	public static final String CURRENCY_WALLET_STATE = "currencyWalletState";
	
    public static final String TRANSACTION_ID = "transactionId";

	public static final String TRANSACTION_AMOUNT = "transactionAmount";
	
	public static final String TRANSACTION_CURRENCY_ID = "transactionCurrencyId";
	
	public static final String TRANSACTION_EXCHANGE_RATE = "transactionExchangeRate";
	
	public static final String DIRECTION = "direction";
	
	public static final String DESCRIPTION = "description";
	
	public static final String AVAILABLE_AMOUNT = "availableAmount"; 
  
}
