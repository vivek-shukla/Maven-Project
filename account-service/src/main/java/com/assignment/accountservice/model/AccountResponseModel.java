package com.assignment.accountservice.model;

import java.util.List;

public class AccountResponseModel {
	
	private Long accountId;
	
	private String customerId;
	
	private List<CurrencyBalance> balances;

	public Long getAccountId() {
		return accountId;
	}

	public void setAccountId(Long accountId) {
		this.accountId = accountId;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public List<CurrencyBalance> getBalances() {
		return balances;
	}

	public void setBalances(List<CurrencyBalance> balances) {
		this.balances = balances;
	}

}
