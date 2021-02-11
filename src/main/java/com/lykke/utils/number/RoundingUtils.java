package com.lykke.utils.number;

import java.text.DecimalFormat;

public class RoundingUtils {
    private static final DecimalFormat FORMAT = initFormat(8);
    private static final DecimalFormat FORMAT2 = initFormat(2);

    private static DecimalFormat initFormat(int accuracy) {
        DecimalFormat df = new DecimalFormat("#.#");
        df.setMaximumFractionDigits(accuracy);
        return df;
    }

    public static String roundForPrint(Double value) {
        return FORMAT.format(value);
    }

    public static String roundForPrint2(Double value) {
        if (value.isNaN()) {
            return "0";
        }
        return FORMAT2.format(value);
    }
}