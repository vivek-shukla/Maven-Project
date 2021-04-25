package com.assignment.accountservice.domain.statechange;

import java.util.List;

import com.assignment.accountservice.model.CurrencyBalance;

public class AccountStateChange { 
	
    private String customerId;
	
	private Long accountId;
	
	private String country;
	
	private Double balance;
	
	private List<CurrencyWalletState> currencyWalletStates;
	
    private String transactionId;
	
	private Double transactionAmount;
	
	private Long transactionCurrencyId;
	
	private Double transactionExchangeRate;
	
	private String direction;
	
	private String description;
	
	private Double availableAmount;

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public Long getAccountId() {
		return accountId;
	}

	public void setAccountId(Long accountId) {
		this.accountId = accountId;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public Double getBalance() {
		return balance;
	}

	public void setBalance(Double balance) {
		this.balance = balance;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}
	
	public Double getTransactionExchangeRate() {
		return transactionExchangeRate;
	}

	public void setTransactionExchangeRate(Double transactionExchangeRate) {
		this.transactionExchangeRate = transactionExchangeRate;
	}

	public String getDirection() {
		return direction;
	}

	public void setDirection(String direction) {
		this.direction = direction;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Double getAvailableAmount() {
		return availableAmount;
	}

	public void setAvailableAmount(Double availableAmount) {
		this.availableAmount = availableAmount;
	}

	public Double getTransactionAmount() {
		return transactionAmount;
	}

	public void setTransactionAmount(Double transactionAmount) {
		this.transactionAmount = transactionAmount;
	}

	public Long getTransactionCurrencyId() {
		return transactionCurrencyId;
	}

	public void setTransactionCurrencyId(Long transactionCurrencyId) {
		this.transactionCurrencyId = transactionCurrencyId;
	}

	public List<CurrencyWalletState> getCurrencyWalletStates() {
		return currencyWalletStates;
	}

	public void setCurrencyWalletStates(List<CurrencyWalletState> currencyWalletStates) {
		this.currencyWalletStates = currencyWalletStates;
	}
	

}
