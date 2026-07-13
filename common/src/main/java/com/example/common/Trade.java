package com.example.common;

/**
 * Representa uma negociacao (trade) de uma acao no mercado financeiro.
 *
 * @param ticker    codigo da acao (ex.: PETR4, VALE3)
 * @param price     preco unitario da negociacao em reais
 * @param quantity  quantidade de acoes negociadas
 * @param timestamp momento da negociacao em epoch millis
 */
public record Trade(String ticker, double price, int quantity, long timestamp) {
}
