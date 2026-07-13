package com.example.streams;

import com.example.common.Trade;

/**
 * Estado agregado de um papel dentro de uma janela de tempo.
 * E imutavel: cada trade gera um novo WindowStats (estilo funcional),
 * o que combina bem com o modelo de agregacao do Kafka Streams.
 */
public record WindowStats(long count,
                          double sumPrice,
                          double sumPriceVolume,
                          long sumVolume,
                          double min,
                          double max) {

    public static WindowStats empty() {
        return new WindowStats(0, 0, 0, 0, Double.MAX_VALUE, Double.MIN_VALUE);
    }

    public WindowStats add(Trade t) {
        return new WindowStats(
                count + 1,
                sumPrice + t.price(),
                sumPriceVolume + t.price() * t.quantity(),
                sumVolume + t.quantity(),
                Math.min(min, t.price()),
                Math.max(max, t.price()));
    }

    public double avgPrice() {
        return count == 0 ? 0 : sumPrice / count;
    }

    public double vwap() {
        return sumVolume == 0 ? 0 : sumPriceVolume / sumVolume;
    }
}
