package com.assignment.accountservice.domain;

public class Transaction {
	
	private String transactionId;
	
	private Long accountId;
	
	private Double amount;
	
	private Long currencyId;
	
	private Double transactionExchangeRate;
	
	private String direction;
	
	private String description;
	
	private Double availableAmount;

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public Long getAccountId() {
		return accountId;
	}

	public void setAccountId(Long accountId) {
		this.accountId = accountId;
	}

	public Double getAmount() {
		return amount;
	}

	public void setAmount(Double amount) {
		this.amount = amount;
	}

	public Long getCurrencyId() {
		return currencyId;
	}

	public void setCurrencyId(Long currencyId) {
		this.currencyId = currencyId;
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
	
	

}
