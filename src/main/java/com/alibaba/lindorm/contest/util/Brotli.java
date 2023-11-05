package com.alibaba.lindorm.contest.util;

import com.nixxcode.jvmbrotli.common.BrotliLoader;
import com.nixxcode.jvmbrotli.enc.BrotliOutputStream;
import com.nixxcode.jvmbrotli.dec.BrotliInputStream;
import com.nixxcode.jvmbrotli.enc.Encoder;

import java.io.*;
import java.nio.ByteBuffer;

public class Brotli {

    static {
        BrotliLoader.isBrotliAvailable();
    }

    public static void compress(ByteBuffer dst, ByteBuffer src) {
        try {
            BrotliOutputStream brotliOutput = new BrotliOutputStream(new ByteBufferOutputStream(dst), new Encoder.Parameters().setQuality(3));
            while(src.hasRemaining()) {
                brotliOutput.write(src.get());
            }
            brotliOutput.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void decompress(ByteBuffer dst, ByteBuffer src) {
        try {
            byte[] bs = new byte[src.remaining()];

            BrotliInputStream brotliInput = new BrotliInputStream(new ByteBufferInputStream(src));

            int b;
            while ((b = brotliInput.read()) != -1) {
                dst.put((byte) b);
            }

            brotliInput.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Helper class to adapt a ByteBuffer to a ReadableByteChannel
    private static class ByteBufferInputStream extends InputStream {
        private final ByteBuffer buffer;

        public ByteBufferInputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() throws IOException {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            return buffer.get() & 0xFF;
        }

        @Override
        public int read(byte[] bytes, int off, int len) throws IOException {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            len = Math.min(len, buffer.remaining());
            buffer.get(bytes, off, len);
            return len;
        }
    }

    // Helper class to adapt a ByteBuffer to a WritableByteChannel
    private static class ByteBufferOutputStream extends OutputStream {
        private final ByteBuffer buffer;

        public ByteBufferOutputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int b) throws IOException {
            buffer.put((byte) b);
        }

        @Override
        public void write(byte[] bytes, int off, int len) throws IOException {
            buffer.put(bytes, off, len);
        }
    }

    public static void main(String[] args) {
        byte[] bs = "Sy4que-8CCmiiCWSyu'eCa4GGmy4\\m4[0e&--yKSGWumeW.\\00#-.'&O-&4qiOq\\O4.\\C8yqK&qaq#OGummaaa0.-GKO'm-0i.[Oi\\8K#0#0\\ySG.SSa[8.OmCiS\\\\mSyayK&aqa..Sm'Ca[[mG-0&-''''4q[muy#G0'KWmSa#4yKme48OmKuO'[4&iG[q#4[u\\44yuq\\y#.mm8C.-K8iS[Gq[uy.-'&WK&u&4SSm'.WayWaO[OeC\\4C00&-8C&yim\\qqaeCO..uCKWaCSO-qS&-0maO-y#W''WSyeWuGy#-'&'O840O'm8iyO''eO48yyeyy[444#e8WGq#mGqmu40[GyWCC8\\&8SG8qi'GO4&Gu.8[8y.\\KOe-meW.8O'#.\\[WK#u&.S[&a#SaS'S4aC\\eu4CSyyiaGCeuq4e8iuee808u-0WeS-#G80yaOS.[4\\q0C\\8&\\y'W\\q#\\uKKWSeC\\aS[u'yyWaq0..'SS0y.yKGqKG.8yCm'4&m0--S.W0q[uam[eu-0K0S#O&-.mu'\\iaOq#0mKyS0[e-iOGSmia4SC4qyyy8ySa4S[ui#[8iS8yi4[iu8u8K[-aqy[u4&\\K0aGK#4SyKW&0.y'-'0m'\\[C&'-OWu.\\'&WCe-\\.Kq\\i[-G'Kae0WmS.m0[8-eSO[iCaK\\mm#Gy.O[i[.yG\\y[a\\#eG..iuSySGa0[aC4iG-ye8[-C\\8i#O#O[aC\\.C-W#0'OOimCu.84\\e'qaO#-\\a\\[8Kuuq'quS8KCKSOqWSKCyW[.iyKeKOiKuy0'.e[\\8OWq#quqK\\#K&SOueGSi&Cqu-.u&S#8CSmeGiuO-[OWSa&88y&iOG[&''eCum-yau-C['&GG\\[imyaOKC&8#q08mi.m[-\\aa-'W8W'4O.eK0[G8.-#-&-&\\8[4WG[umO8#OO-K[u8ae&4C'&-0&q[mGS\\0'SyG.Saei0\\0#SOeW4S[WK#4CuqC[[4Sy\\-S0.#0GqyW0['e.CO''m.-qK-8.Su#&'eWe0#m'..m&.Ki#Wu'4.\\O8'#O8m#4-#iaO'Oq-CK4i&iaWqqySe8imC\\KCy[Cq0q\\0.eu8.iyya\\ymS4aW[CCqW.ym.KCCa.imem[K#&.[8m0Wim-S[-#u'uC4qS#&8ya4&4&q'-S\\0#CGCq-'[m0uC[i0aCaqi#\\-y4-q#&&Wi0.4q-Cq8a0\\mGqmm.mi-G&'GyKm8C4Kq&K#&4[#[qGy#S4e.Ge'Cy&u-e0mu&CaW4ymi#i&.[8yy'&u4'iq[CS-[-uGa.W8[8yaaGi4#\\O'eyKa8uK\\8a..WamS['C0KS0Om[GiC#4&['G[W.i0CuyKOC[i[e-WCS8'-aCyW8[-ay8''OC\\&a8i&0-GKK0.K88[uqGy#aCSq.ye'iOe.Gmea.'4uyy#8KC0KaWS\\Wq'&q\\[\\[CmS0WiO.G\\KWy4uyWS0y&qOW4&-mqa&W#OyGuOG4a'8[8ySaC.4-0['4yGS0CG&8i\\G.Wqq'OCG44SyWSSK0K\\WyWGu-yy[u-\\-Ce.\\0iu-CO04Syme.Ky\\#08WaquGKKi'44qaq0eq[iq8u'#a.\\.0#yu-4G#0-OuGW-meu#ai&0C".getBytes();

        ByteBuffer src = ByteBuffer.allocateDirect(bs.length);
        src.put(bs);
        src.flip();


        ByteBuffer encode = ByteBuffer.allocateDirect(bs.length);
        compress(encode, src);
        encode.flip();

        System.out.println(encode.remaining() + "/" + bs.length + "=" + (encode.remaining() * 100 / bs.length) + "%");

        ByteBuffer decode = ByteBuffer.allocateDirect(bs.length);
        decompress(decode, encode);
        decode.flip();

        byte[] bs2 = new byte[decode.remaining()];
        decode.get(bs2);
        System.out.println(new String(bs2));



    }
}
