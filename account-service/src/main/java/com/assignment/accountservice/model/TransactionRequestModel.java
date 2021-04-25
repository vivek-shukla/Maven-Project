package com.assignment.accountservice.model;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class TransactionRequestModel {
	
	@NotNull
	private Long accountId;
	
	@NotNull
	@Min(0)
	private Double amount;
	
	private String currency;
	
	private String directionOfTransaction;
	
	@NotEmpty
	private String description;
	
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
	
	

}
