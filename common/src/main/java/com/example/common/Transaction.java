package com.example.common;

/**
 * Uma transacao financeira - o evento que circula no topico 'transactions'.
 *
 * @param id        identificador da transacao (ex.: TX-0042)
 * @param accountId conta que realizou a transacao (ex.: ACC-003)
 * @param amount    valor em reais
 * @param timestamp momento da transacao (epoch millis)
 */
public record Transaction(String id, String accountId, double amount, long timestamp) {
}
