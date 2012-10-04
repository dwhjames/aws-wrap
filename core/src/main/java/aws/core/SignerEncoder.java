package aws.core;

/**
TOOK THIS FROM TYPICA 
http://code.google.com/p/typica/source/browse/tags/1.5.1/java/com/xerox/amazonws/common/SignerEncoder.java
**/


import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.BitSet;
import java.security.AccessController;
import sun.security.action.GetPropertyAction;

/**
 * This encoder is to be used instead of the URLEncoder when encoding params for version 2 signing
 */
public class SignerEncoder {
        private static BitSet dontEncode;
        private static String defaultEncoding;

        static {
                // encode everything except what is included in the bitset
                dontEncode = new BitSet(256);
                for (int i='a'; i<='z'; i++) {
                        dontEncode.set(i);
                }
                for (int i='A'; i<='Z'; i++) {
                        dontEncode.set(i);
                }
                for (int i='0'; i<='9'; i++) {
                        dontEncode.set(i);
                }
                dontEncode.set('-');
                dontEncode.set('_');
                dontEncode.set('.');

                defaultEncoding = (String)AccessController.doPrivileged(new GetPropertyAction("file.encoding"));
        }

        public static String encode(String str) {
                int lowerDiff = 'a' - 'A';
                StringBuffer ret = new StringBuffer(str.length());
                try {
                        byte [] src = str.getBytes(defaultEncoding);
                        for (int pos = 0; pos < src.length; pos++) {
                                int chr = (int)str.charAt(pos);
                                if (dontEncode.get(chr)) {
                                        ret.append((char)chr);
                                }
                                else {
                                        ret.append("%");
                                        char ch = Character.forDigit((src[pos] >> 4) & 0xf, 16);
                                        if (Character.isLetter(ch)) { ch -= lowerDiff; }
                                        ret.append(ch);
                                        ch = Character.forDigit(src[pos] & 0xf, 16);
                                        if (Character.isLetter(ch)) { ch -= lowerDiff; }
                                        ret.append(ch);
                                }
                        }
                } catch (UnsupportedEncodingException ex) { }
                return ret.toString();
        }
}
