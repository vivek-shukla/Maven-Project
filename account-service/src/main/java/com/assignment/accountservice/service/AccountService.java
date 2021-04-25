package com.assignment.accountservice.service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.assignment.accountservice.domain.Account;
import com.assignment.accountservice.domain.CurrencyExchangeRate;
import com.assignment.accountservice.domain.CurrencyWallet;
import com.assignment.accountservice.domain.Transaction;
import com.assignment.accountservice.domain.statechange.AccountStateChange;
import com.assignment.accountservice.domain.statechange.CurrencyWalletState;
import com.assignment.accountservice.exception.AccountServiceException;
import com.assignment.accountservice.mapper.AccountMapper;
import com.assignment.accountservice.mapper.CurrencyExchangeRateMapper;
import com.assignment.accountservice.mapper.CurrencyWallletMapper;
import com.assignment.accountservice.mapper.TransactionMapper;
import com.assignment.accountservice.model.AccountRequestModel;
import com.assignment.accountservice.model.AccountResponseModel;
import com.assignment.accountservice.model.CurrencyBalance;
import com.assignment.accountservice.model.TransactionRequestModel;
import com.assignment.accountservice.model.TransactionResponseModel;
import com.assignment.accountservice.util.Constants;
import com.assignment.accountservice.util.Validator;
import com.assignment.accountservice.util.ErrorCode;

@Service
public class AccountService {
	
	@Autowired
	private AccountMapper accountMapper;
	
	@Autowired
	private CurrencyExchangeRateMapper currencyExchangeRateMapper;
	
	@Autowired
	private CurrencyWallletMapper currencyWallletMapper;
	
	@Autowired
	private TransactionMapper transactionMapper;
	
	@Autowired
	private RabbitTemplate template;
    
	private static final Logger LOGGER = LoggerFactory.getLogger(AccountService.class);
	
	
	public AccountResponseModel createAccount(AccountRequestModel accountDTO) throws AccountServiceException {
		
		Validator.validateCurrency(accountDTO.getCurrencies());
		Account account = new Account();
		account.setCustomerId(accountDTO.getCustomerId());
		account.setBalance(Constants.ZERO_DOUBLE);
		account.setCountry(accountDTO.getCountry());
		accountMapper.insert(account);
		account.setAccountId(accountMapper.findByCustomerId(accountDTO.getCustomerId()));
		return populateCurrencyWalletAndResponseModel(account,accountDTO.getCurrencies());
	}

	private AccountResponseModel populateCurrencyWalletAndResponseModel(Account account, List<String> currencies) throws AccountServiceException{
		
		List<CurrencyExchangeRate> currencyExchangeRates = currencyExchangeRateMapper.findAll();		
		for(String currencyCode: currencies) {
			LOGGER.info("Currency Exchance Rate:: {}",currencyExchangeRates);
			CurrencyExchangeRate currencyExchangeRate = currencyExchangeRates.stream()
					.filter(cer -> currencyCode.equalsIgnoreCase(cer.getCurrencyCode())).findFirst()
					.orElseThrow(() -> new AccountServiceException(ErrorCode.INVALID_CURRENCY, Constants.INVALID_CURRENCY));
		    CurrencyWallet currencyWallet = new CurrencyWallet();
		    currencyWallet.setAccountId(account.getAccountId());
		    currencyWallet.setCurrencyId(currencyExchangeRate.getCurrencyId());
		    currencyWallletMapper.insert(currencyWallet);
		}
		sendAsync(account);
		return getAccountResponseModel().apply(account);
		
		
	}

	public AccountResponseModel fetchByAccountId(Long accountId) throws AccountServiceException {		
		return Optional.ofNullable(accountMapper.findByAccountId(accountId))
				.map(getAccountResponseModel())
				.orElseThrow(() -> new AccountServiceException(ErrorCode.ACCOUNT_NOT_FOUND, Constants.ACCOUNT_NOT_FOUND));
	}

	private Function<Account,AccountResponseModel> getAccountResponseModel() {
		return acc -> {
			AccountResponseModel accountResponseModel = new AccountResponseModel();
			accountResponseModel.setAccountId(acc.getAccountId());
			accountResponseModel.setCustomerId(acc.getCustomerId());
			accountResponseModel.setBalances(getBalancesForAccount(acc));
			return accountResponseModel;		
		};
	}

	private List<CurrencyBalance> getBalancesForAccount(Account acc) {		
		List<Long> currencies = currencyWallletMapper.getCurrenciesforAccount(acc.getAccountId());	
		return currencyExchangeRateMapper.findAll().stream().filter(crr -> currencies.contains(crr.getCurrencyId())).map(crr -> {
				CurrencyBalance balance = new CurrencyBalance();
				balance.setBalance(getRoundFigure(acc.getBalance() * crr.getExchangeRate()));
				balance.setCurrency(crr.getCurrencyCode());
				return balance;
		}).collect(Collectors.toList());
	}

	public TransactionResponseModel createTransaction(TransactionRequestModel transactionModel) throws AccountServiceException {
		
		String transactionDirection = transactionModel.getDirectionOfTransaction();
		Account account = accountMapper.findByAccountId(transactionModel.getAccountId());
		Validator.staticValidationForTransactionRequest(transactionModel, account);
		validateFundsAndCurrency(transactionModel, account);
		CurrencyExchangeRate currencyExchangeRate = currencyExchangeRateMapper.findAll().stream().
				 filter(cer -> cer.getCurrencyCode().equalsIgnoreCase(transactionModel.getCurrency()))
				.findAny().orElseThrow(null);
		Double amountInUSD = currencyExchangeRate.getInverseExchangeRate() * transactionModel.getAmount();
		Double currentBalance = account.getBalance();
		if(Constants.IN.equalsIgnoreCase(transactionDirection)) {
			currentBalance = currentBalance + amountInUSD;
		} else if (Constants.OUT.equalsIgnoreCase(transactionDirection)) {
			currentBalance = currentBalance - amountInUSD;
		}
		account.setBalance(currentBalance);
		Transaction transaction = new Transaction();
		transaction.setTransactionId(UUID.randomUUID().toString());
		transaction.setAccountId(transactionModel.getAccountId());
		transaction.setCurrencyId(currencyExchangeRate.getCurrencyId());
		transaction.setAmount(transactionModel.getAmount());
		transaction.setTransactionExchangeRate(currencyExchangeRate.getExchangeRate());
		transaction.setDirection(transactionModel.getDirectionOfTransaction());
		transaction.setDescription(transactionModel.getDescription());
		transaction.setAvailableAmount(account.getBalance());
		updateAndSendAsync(transaction,account);
		return getTransactionResponseModel(transaction,currencyExchangeRate);
	}

	private void validateFundsAndCurrency(TransactionRequestModel transactionModel, Account account) throws AccountServiceException {
		String transactionCurrency = transactionModel.getCurrency();
		Double transactionAmount = transactionModel.getAmount();
		List<String> validCurrencyCodes = currencyWallletMapper.getCurrencyCodesforAccount(account.getAccountId());		
		if(!CollectionUtils.isEmpty(validCurrencyCodes) && !validCurrencyCodes.contains(transactionCurrency)) {
			LOGGER.debug("Invalid Transaction Currency :: {}",transactionCurrency);
			throw new AccountServiceException(ErrorCode.INVALID_CURRENCY,Constants.INVALID_CURRENCY);
		}
		/*if(currencyExchangeRateMapper.findAll().stream()
				.noneMatch(cer -> cer.getCurrencyCode().equalsIgnoreCase(transactionCurrency))) {
			LOGGER.debug("Invalid Transaction Currency :: {}",transactionCurrency);
			throw new AccountServiceException(ErrorCode.INVALID_CURRENCY,Constants.INVALID_CURRENCY);
		}
		*/
		List<CurrencyBalance> balances = getBalancesForAccount(account);
		if(Constants.OUT.equalsIgnoreCase(transactionModel.getDirectionOfTransaction()) && 
				balances.stream().anyMatch(bal-> bal.getCurrency().equalsIgnoreCase(transactionCurrency) && transactionAmount > bal.getBalance())) {
			LOGGER.debug("Insufficient funds for transaction:: {}", transactionAmount);
			throw new AccountServiceException(ErrorCode.INSUFFICIENT_FUNDS,Constants.INSUFFICIENT_FUNDS);			
		}
	}

	private TransactionResponseModel getTransactionResponseModel(Transaction transaction, CurrencyExchangeRate currencyExchangeRate) {
		TransactionResponseModel transactionResponseModel = new TransactionResponseModel();
		transactionResponseModel.setAccountId(transaction.getAccountId());
		transactionResponseModel.setAmount(transaction.getAmount());
		transactionResponseModel.setBalanceAfterTransaction(getRoundFigure(transaction.getAvailableAmount() * transaction.getTransactionExchangeRate()));
		transactionResponseModel.setCurrency(currencyExchangeRate.getCurrencyCode());
		transactionResponseModel.setDescription(transaction.getDescription());
		transactionResponseModel.setDirectionOfTransaction(transaction.getDirection());
		transactionResponseModel.setTransactionId(transaction.getTransactionId());
		return transactionResponseModel;
	}
	
	
	
	private Double getRoundFigure(Double d) {
		return Math.round(d) * Constants.HUNDRED_DOUBLE / Constants.HUNDRED_DOUBLE;
	}

	public List<TransactionResponseModel> fetchTransactionByAccountId(Long accountId) throws AccountServiceException {
		List<Transaction> transactions = transactionMapper.findByAccountId(accountId);
		if(!CollectionUtils.isEmpty(transactions)) {
			return transactions.stream().map(tr -> {
				return getTransactionResponseModel(tr,currencyExchangeRateMapper.findAll().stream()
						.filter(cer -> tr.getCurrencyId() == cer.getCurrencyId()).findAny().orElse(null));
			}).collect(Collectors.toList());
		}
		else {
			throw new AccountServiceException(ErrorCode.ACCOUNT_NOT_FOUND, Constants.ACCOUNT_MISSING);
		}
	}
	
	private void sendAsync(Account account) {		
		template.convertAndSend(Constants.TOPIC_EXCHANGE, Constants.ROUTING_KEY, getaccountStateChange(account,null));
	}
	
	private void updateAndSendAsync(Transaction transaction, Account account) {
		transactionMapper.insert(transaction);
		accountMapper.updateAccount(account);
		template.convertAndSend(Constants.TOPIC_EXCHANGE, Constants.ROUTING_KEY, getaccountStateChange(account,transaction));
		
	}

	private AccountStateChange getaccountStateChange(Account account, Transaction transaction) {
		AccountStateChange accountStateChange = new AccountStateChange();
		accountStateChange.setAccountId(account.getAccountId());
		accountStateChange.setCustomerId(account.getCustomerId());
		accountStateChange.setCountry(account.getCountry());
		accountStateChange.setBalance(account.getBalance());
		accountStateChange.setCurrencyWalletStates(captureCurrencyWalletStateForAccount(account));
		if(transaction != null) {
			accountStateChange.setTransactionId(transaction.getTransactionId());
			accountStateChange.setTransactionAmount(transaction.getAmount());
			accountStateChange.setTransactionExchangeRate(transaction.getTransactionExchangeRate());
			accountStateChange.setDirection(transaction.getDirection());
			accountStateChange.setDescription(transaction.getDescription());
			accountStateChange.setAvailableAmount(transaction.getAvailableAmount());
			accountStateChange.setTransactionCurrencyId(transaction.getCurrencyId());
		}
		return accountStateChange;
	}
	
	private List<CurrencyWalletState> captureCurrencyWalletStateForAccount(Account acc) {		
		List<Long> currencies = currencyWallletMapper.getCurrenciesforAccount(acc.getAccountId());	
		return currencyExchangeRateMapper.findAll().stream().filter(crr -> currencies.contains(crr.getCurrencyId())).map(crr -> {
				CurrencyWalletState currencyWalletState = new CurrencyWalletState();
				currencyWalletState.setCurrencyId(crr.getCurrencyId());
				currencyWalletState.setCurrency(crr.getCurrencyCode());
				currencyWalletState.setExchangeRate(crr.getExchangeRate());
				currencyWalletState.setInverseExchangeRate(crr.getInverseExchangeRate());
				currencyWalletState.setBalance(getRoundFigure(acc.getBalance() * crr.getExchangeRate()));
				return currencyWalletState;
		}).collect(Collectors.toList());
	}

}
