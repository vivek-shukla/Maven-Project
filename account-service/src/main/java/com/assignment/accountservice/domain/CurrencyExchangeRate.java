package com.assignment.accountservice.domain;

public class CurrencyExchangeRate {
	
	private Long currencyId;
	
	private String currencyCode;
	
	private Double exchangeRate;
	
	private Double inverseExchangeRate;

	public Double getInverseExchangeRate() {
		return inverseExchangeRate;
	}

	public void setInverseExchangeRate(Double inverseExchangeRate) {
		this.inverseExchangeRate = inverseExchangeRate;
	}

	public Long getCurrencyId() {
		return currencyId;
	}

	public void setCurrencyId(Long currencyId) {
		this.currencyId = currencyId;
	}

	public String getCurrencyCode() {
		return currencyCode;
	}

	public void setCurrencyCode(String currencyCode) {
		this.currencyCode = currencyCode;
	}

	public Double getExchangeRate() {
		return exchangeRate;
	}

	public void setExchangeRate(Double exchangeRate) {
		this.exchangeRate = exchangeRate;
	}
	
	

}
