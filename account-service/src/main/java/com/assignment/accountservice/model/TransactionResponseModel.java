package com.assignment.accountservice.model;

public class TransactionResponseModel {
	
	private Long accountId;
	
	private String transactionId;
	
	private Double amount;
	
	private String currency;
	
	private String directionOfTransaction;
	
	private String description;
	
	public Long getAccountId() {
		return accountId;
	}

	public void setAccountId(Long accountId) {
		this.accountId = accountId;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public Double getAmount() {
		return amount;
	}

	public void setAmount(Double amount) {
		this.amount = amount;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public String getDirectionOfTransaction() {
		return directionOfTransaction;
	}

	public void setDirectionOfTransaction(String directionOfTransaction) {
		this.directionOfTransaction = directionOfTransaction;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Double getBalanceAfterTransaction() {
		return balanceAfterTransaction;
	}

	public void setBalanceAfterTransaction(Double balanceAfterTransaction) {
		this.balanceAfterTransaction = balanceAfterTransaction;
	}

	private Double balanceAfterTransaction;

}
