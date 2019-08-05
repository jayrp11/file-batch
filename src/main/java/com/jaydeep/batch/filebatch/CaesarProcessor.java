package com.jaydeep.batch.filebatch;

import org.springframework.batch.item.ItemProcessor;

public class CaesarProcessor implements ItemProcessor<String, String> {
    @Override
    public String process(String str) {
        StringBuilder strBuilder = new StringBuilder();
        int shift = 2;
        char c;
        for (int i = 0; i < str.length(); i++) {
            c = str.charAt(i);
            if (Character.isLetter(c)) {
                c = (char) (str.charAt(i) + shift);
                if ((Character.isLowerCase(str.charAt(i)) && c > 'z')
                        || (Character.isUpperCase(str.charAt(i)) && c > 'Z'))
                    c = (char) (str.charAt(i) - (26 - shift));
            }
            strBuilder.append(c);
        }
        return strBuilder.toString();
    }
}
