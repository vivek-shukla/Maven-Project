package com.assignment.accountservice.domain.statechange;

public class CurrencyWalletState {
	
	private Long currencyId;
	
    private String currency;
	
	private Double balance;
	
    private Double exchangeRate;
	
	private Double inverseExchangeRate;

	public Long getCurrencyId() {
		return currencyId;
	}

	public void setCurrencyId(Long currencyId) {
		this.currencyId = currencyId;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public Double getBalance() {
		return balance;
	}

	public void setBalance(Double balance) {
		this.balance = balance;
	}

	public Double getExchangeRate() {
		return exchangeRate;
	}

	public void setExchangeRate(Double exchangeRate) {
		this.exchangeRate = exchangeRate;
	}

	public Double getInverseExchangeRate() {
		return inverseExchangeRate;
	}

	public void setInverseExchangeRate(Double inverseExchangeRate) {
		this.inverseExchangeRate = inverseExchangeRate;
	}
}
