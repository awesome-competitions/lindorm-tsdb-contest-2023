package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Brotli;
import com.alibaba.lindorm.contest.v2.Context;
import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;

public class StringCodec extends Codec<ByteBuffer[]>{

    private final int fixedSize;

    private final int maxSize;

    public StringCodec(int fixedSize) {
        this(fixedSize, 100);
    }

    public StringCodec(int fixedSize, int maxSize) {
        this.fixedSize = fixedSize;
        this.maxSize = maxSize;
    }

    @Override
    public void encode(ByteBuffer src, ByteBuffer[] data, int size) {
        ByteBuffer encodeBuffer = Context.getCodecEncodeBuffer().clear();
        for (int i = 0; i < size; i++) {
            ByteBuffer buffer = data[i];
            if (fixedSize == 0){
                if (maxSize < 128){
                    encodeBuffer.put((byte) buffer.remaining());
                }else{
                    encodeBuffer.putShort((short) buffer.remaining());
                }
            }
            encodeBuffer.put(buffer);
        }
        encodeBuffer.flip();
        Brotli.compress(src, encodeBuffer);
    }

    @Override
    public void decode(ByteBuffer src, ByteBuffer[] data, int size) {
        ByteBuffer decodeBuffer = Context.getCodecDecodeBuffer().clear();
        Brotli.decompress(decodeBuffer, src);
        decodeBuffer.flip();

        for (int i = 0; i < size; i++) {
            int capacity = fixedSize;
            if (capacity == 0){
                if (maxSize < 128){
                    capacity = decodeBuffer.get();
                }else{
                    capacity = decodeBuffer.getShort();
                }
            }
            ByteBuffer val = ByteBuffer.allocate(capacity);
            decodeBuffer.get(val.array(), 0, val.limit());
            data[i] = val;
        }
    }

    public static void main(String[] args) {
        StringCodec bc = new StringCodec(0, 5000);

//        String str = "i\\8K#0#0\\ySG.SSa[8.OmCiS\\\\mSyayK&aqa..Sm'Ca[[mG-0&-''''4q[muy#G0'KWmSa#4yKme48OmKuO'[4&iG[q#4[u\\44yu9q\\y#.mm8C.-K8iS[Gq[uy.-'&WK&u&4SSm'.WayWaO[OeC\\4C00&-8C&yim\\qqaeCO..uCKWaCSO-qS&-0maO-y#W''WSyeWuGy#9-'&'O840O'm8iyO''eO48yyeyy[444#e8WGq#mGqmu40[GyWCC8\\&8SG8qi'GO4&Gu.8[8y.\\KOe-meW.8O'#.\\[WK#u&.S[&a#S9aS'S4aC\\eu4CSyyiaGCeuq4e8iuee808u-0WeS-#G80yaOS.[4\\q0C\\8&\\y'W\\q#\\uKKWSeC\\aS[u'yyWaq0..'SS0y.yKGqKG.89yCm'4&m0--S.W0q[uam[eu-0K0S#O&-.mu'\\iaOq#0mKyS0[e-iOGSmia4SC4qyyy8ySa4S[ui#[8iS8yi4[iu8u8K[-aqy[u4&\\9K0aGK#4SyKW&0.y'-'0m'\\[C&'-OWu.\\'&WCe-\\.Kq\\i[-G'Kae0WmS.m0[8-eSO[iCaK\\mm#Gy.O[i[.yG\\y[a\\#eG..iuSySGa90[aC4iG-ye8[-C\\8i#O#O[aC\\.C-W#0'OOimCu.84\\e'qaO#-\\a\\[8Kuuq'quS8KCKSOqWSKCyW[.iyKeKOiKuy0'.e[\\8OWq#qu9qK\\#K&SOueGSi&Cqu-.u&S#8CSmeGiuO-[OWSa&88y&iOG[&''eCum-yau-C['&GG\\[imyaOKC&8#q08mi.m[-\\aa-'W8W'4O.eK90[G8.-#-&-&\\8[4WG[umO8#OO-K[u8ae&4C'&-0&q[mGS\\0'SyG.Saei0\\0#SOeW4S[WK#4CuqC[[4Sy\\-S0.#0GqyW0['e.CO''9m.-qK-8.Su#&'eWe0#m'..m&.Ki#Wu'4.\\O8'#O8m#4-#iaO'Oq-CK4i&iaWqqySe8imC\\KCy[Cq0q\\0.eu8.iyya\\ymS4aW[CCq9W.ym.KCCa.imem[K#&.[8m0Wim-S[-#u'uC4qS#&8ya4&4&q'-S\\0#CGCq-'[m0uC[i0aCaqi#\\-y4-q#&&Wi0.4q-Cq8a0\\mGqm9m.mi-G&'GyKm8C4Kq&K#&4[#[qGy#S4e.Ge'Cy&u-e0mu&CaW4ymi#i&.[8yy'&u4'iq[CS-[-uGa.W8[8yaaGi4#\\O'eyKa8uK\\98a..WamS['C0KS0Om[GiC#4&['G[W.i0CuyKOC[i[e-WCS8'-aCyW8[-ay8''OC\\&a8i&0-GKK0.K88[uqGy#aCSq.ye'iOe.Gme9a.'4uyy#8KC0KaWS\\Wq'&q\\[\\[CmS0WiO.G\\KWy4uyWS0y&qOW4&-mqa&W#OyGuOG4a'8[8ySaC.4-0['4yGS0CG&8i\\G.Wqq'OC9G44SyWSSK0K\\WyWGu-yy[u-\\-Ce.\\0iu-CO04Syme.Ky\\#08WaquGKKi'44qaq0eq[iq8u'#a.\\.0#yu-4G#0-OuGW-meu#ai&0C";
//        String str = "GmyKO8&y.SyuSm8Sy4que-8CCmiiCWSyu'eCa4GGmy4\\m4[0e&--yKSGWumeW.\\00#-.'&O-&4qiOq\\O4.\\C8yqK&qaq#OGummaaa0.-GKO'm-0i.[O##u-[me.m4SO-uCOC0-i\\qmCauSiO8#iOS0K00-Oa\\mCWe8i&q#&OuC'8yK#C.CeC";

        String[] strings = new String[]{
                "GmyKO8&y.SyuSm8.&CeGqu&iO4e'C4e&''4&8y.mGW4'48G.mi#0&ii#Gei0\\Gm#y4[S#KqaOW.mqGmyGiqO.eiSK\\yOu0e&08'0iOW-.O#uWy#\\K'uu.qm0eyi'e-a4\\u'4SaOO#0C4K8.K.\\a48Ca[aKe[40mK[O\\C8K[SG4O['8mu[a.Wa[GCO.yWaO.y4mCW\\4qqeiiei\\#G&y\\[m[\\C-CCa\\#G[8uSy#'mOie&WaSWu",
                "Sy4que-8CCmiiCWSyu'eCa4GGmy4\\m4[0e&--yKSGWumeW.\\00#-.'&O-&4qiOq\\O4.\\C8yqK&qaq#OGummaaa0.-GKO'm-0i.[Oi\\8K#0#0\\ySG.SSa[8.OmCiS\\\\mSyayK&aqa..Sm'Ca[[mG-0&-''''4q[muy#G0'KWmSa#4yKme48OmKuO'[4&iG[q#4[u\\44yuq\\y#.mm8C.-K8iS[Gq[uy.-'&WK&u&4SSm'.WayWaO[OeC\\4C00&-8C&yim\\qqaeCO..uCKWaCSO-qS&-0maO-y#W''WSyeWuGy#-'&'O840O'm8iyO''eO48yyeyy[444#e8WGq#mGqmu40[GyWCC8\\&8SG8qi'GO4&Gu.8[8y.\\KOe-meW.8O'#.\\[WK#u&.S[&a#SaS'S4aC\\eu4CSyyiaGCeuq4e8iuee808u-0WeS-#G80yaOS.[4\\q0C\\8&\\y'W\\q#\\uKKWSeC\\aS[u'yyWaq0..'SS0y.yKGqKG.8yCm'4&m0--S.W0q[uam[eu-0K0S#O&-.mu'\\iaOq#0mKyS0[e-iOGSmia4SC4qyyy8ySa4S[ui#[8iS8yi4[iu8u8K[-aqy[u4&\\K0aGK#4SyKW&0.y'-'0m'\\[C&'-OWu.\\'&WCe-\\.Kq\\i[-G'Kae0WmS.m0[8-eSO[iCaK\\mm#Gy.O[i[.yG\\y[a\\#eG..iuSySGa0[aC4iG-ye8[-C\\8i#O#O[aC\\.C-W#0'OOimCu.84\\e'qaO#-\\a\\[8Kuuq'quS8KCKSOqWSKCyW[.iyKeKOiKuy0'.e[\\8OWq#quqK\\#K&SOueGSi&Cqu-.u&S#8CSmeGiuO-[OWSa&88y&iOG[&''eCum-yau-C['&GG\\[imyaOKC&8#q08mi.m[-\\aa-'W8W'4O.eK0[G8.-#-&-&\\8[4WG[umO8#OO-K[u8ae&4C'&-0&q[mGS\\0'SyG.Saei0\\0#SOeW4S[WK#4CuqC[[4Sy\\-S0.#0GqyW0['e.CO''m.-qK-8.Su#&'eWe0#m'..m&.Ki#Wu'4.\\O8'#O8m#4-#iaO'Oq-CK4i&iaWqqySe8imC\\KCy[Cq0q\\0.eu8.iyya\\ymS4aW[CCqW.ym.KCCa.imem[K#&.[8m0Wim-S[-#u'uC4qS#&8ya4&4&q'-S\\0#CGCq-'[m0uC[i0aCaqi#\\-y4-q#&&Wi0.4q-Cq8a0\\mGqmm.mi-G&'GyKm8C4Kq&K#&4[#[qGy#S4e.Ge'Cy&u-e0mu&CaW4ymi#i&.[8yy'&u4'iq[CS-[-uGa.W8[8yaaGi4#\\O'eyKa8uK\\8a..WamS['C0KS0Om[GiC#4&['G[W.i0CuyKOC[i[e-WCS8'-aCyW8[-ay8''OC\\&a8i&0-GKK0.K88[uqGy#aCSq.ye'iOe.Gmea.'4uyy#8KC0KaWS\\Wq'&q\\[\\[CmS0WiO.G\\KWy4uyWS0y&qOW4&-mqa&W#OyGuOG4a'8[8ySaC.4-0['4yGS0CG&8i\\G.Wqq'OCG44SyWSSK0K\\WyWGu-yy[u-\\-Ce.\\0iu-CO04Syme.Ky\\#08WaquGKKi'44qaq0eq[iq8u'#a.\\.0#yu-4G#0-OuGW-meu#ai&0C",
                "##u-[me.m4SO-uCOC0-i\\qmCauSiO8CC#a&8.\\#0y#8Ky.CO[S#S4OW.G.yWKCCC08K.my0&'a[GK.yyKmC\\K44u-yO'GWSqC\\yu'a-q-\\.KC8uO-a'Geiqmu'Wi8i0q\\iK8&44ee#q8GGmaCqOWeq'&&W8yK\\48y&uSW-y-a\\4KCWei#qO[4a4.OuCGi#Ci[\\4\\[#O&Se-yKaaGW&-y4'u44ii'O4Suu4meCCaa.yWeWyW-K[SCay&Ce'm[-8imy0[[C8u4K0GW'4-y.#a\\.i08Wu'&.S[4iqa[8KiC4CuqGKK-aCGyCWeiqK\\a4S0.imi#4'CqW&qK\\&a'04[yOSCeqG&W[\\-CiS&ua.KCGK&#K\\auqmq'Gu.GuiyCC-G8S#0q#aCO000W[-u-iG8['Kqqu#\\\\m#SO0CWS8KWu[-WaS#iGi'CO'\\eWCG[-WGW4e8ae8iC4&-uO&8u4\\Cy0#\\KKC[\\[#OOi",
                "#iOS0K00-Oa\\mCWe8i&q#&OuC'8yK#C.CeCCmS#e'eSGK-#aSSm8.-0W4'G[8i#-0.WSa&KmiaC'S0-iiqyie.&SuGu#4-#CGK&CC4-mSO[-a\\a&mu-OKS'eOamyOS&Gy&e0i'ueu[mqKq.W'8OSWiuC&G.G8-W''8iy[8K8mqqSO'4\\4#auyGW-mS0W-.W'Sy'-&00&.4C.KO.qOGGO'KCSi\\uSG[4K[u&SKK&W[Wq.[equKyCq8qy#&iS-8a8&i-G04-8Kq.\\##&&.4[COuS0y[K0[q[&#OSyyeqK8#a&W[G#&8ym&CC4i-Gm-WWyyiy\\8'0W[a-8&.CmS#u'e8yKqiqmey[.WCK0-e&uGu4e&0O\\.G0iWeWq.aWe'y.4'qS'Su4&#\\amu&uiaCq8uSi.SimK.We-K4\\.K4'qmGGKGG-.Kmmiy[.#'#CqW0eu[mG80-.&'ueCq#G4.4\\[-[\\-'KC44GOSm0W#qu#aa4-#CeeW-#eu[Ou-CS0e440W4GOaWeyKaemS##[4#-amuu&a\\OG4\\yK\\yy80yi'4i8Ku.4&&iOqKm-GWCa4'[O"
        };

        int total1 = 0, total2 = 0;
        for (String str: strings) {
            ByteBuffer src = ByteBuffer.allocateDirect(10000);
            ByteBuffer data = ByteBuffer.allocateDirect(10000);
            data.put(str.getBytes());
            data.flip();

            bc.encode(src, new ByteBuffer[]{data}, 1);
            src.flip();

            System.out.println(src.remaining() + "/" + str.length() + "=" + (src.remaining() * 1.0 / str.length()));
            total1 += src.remaining();
            total2 += str.length();
        }
        System.out.println("single compress: " + total1 + "/" + total2 + "=" + (total1 * 1.0 / total2));

        String str1 = "";
        for (String str: strings) {
            str1 += str;
        }
        ByteBuffer src = ByteBuffer.allocateDirect(10000);
        ByteBuffer data = ByteBuffer.allocateDirect(10000);
        data.put(str1.getBytes());
        data.flip();

        bc.encode(src, new ByteBuffer[]{data}, 1);
        src.flip();

        System.out.println("multi compress: " + src.remaining() + "/" + str1.length() + "=" + (src.remaining() * 1.0 / str1.length()));

    }
}
