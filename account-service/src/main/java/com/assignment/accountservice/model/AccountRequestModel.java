package com.assignment.accountservice.model;

import java.util.List;

import javax.validation.constraints.NotNull;

public class AccountRequestModel {
	
	@NotNull
	private String customerId;
	
	private String country;
	
	private List<String> currencies;

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public List<String> getCurrencies() {
		return currencies;
	}

	public void setCurrencies(List<String> currencies) {
		this.currencies = currencies;
	}
	
	
	
	

}
