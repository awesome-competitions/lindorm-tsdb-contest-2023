package com.alibaba.lindorm.contest.v2.tests;

public class TestStringCompression {

    public static void main(String[] args) {
        String str = "i\\8K#0#0\\ySG.SSa[8.OmCiS\\\\mSyayK&aqa..Sm'Ca[[mG-0&-''''4q[muy#G0'KWmSa#4yKme48OmKuO'[4&iG[q#4[u\\44yu9q\\y#.mm8C.-K8iS[Gq[uy.-'&WK&u&4SSm'.WayWaO[OeC\\4C00&-8C&yim\\qqaeCO..uCKWaCSO-qS&-0maO-y#W''WSyeWuGy#9-'&'O840O'm8iyO''eO48yyeyy[444#e8WGq#mGqmu40[GyWCC8\\&8SG8qi'GO4&Gu.8[8y.\\KOe-meW.8O'#.\\[WK#u&.S[&a#S9aS'S4aC\\eu4CSyyiaGCeuq4e8iuee808u-0WeS-#G80yaOS.[4\\q0C\\8&\\y'W\\q#\\uKKWSeC\\aS[u'yyWaq0..'SS0y.yKGqKG.89yCm'4&m0--S.W0q[uam[eu-0K0S#O&-.mu'\\iaOq#0mKyS0[e-iOGSmia4SC4qyyy8ySa4S[ui#[8iS8yi4[iu8u8K[-aqy[u4&\\9K0aGK#4SyKW&0.y'-'0m'\\[C&'-OWu.\\'&WCe-\\.Kq\\i[-G'Kae0WmS.m0[8-eSO[iCaK\\mm#Gy.O[i[.yG\\y[a\\#eG..iuSySGa90[aC4iG-ye8[-C\\8i#O#O[aC\\.C-W#0'OOimCu.84\\e'qaO#-\\a\\[8Kuuq'quS8KCKSOqWSKCyW[.iyKeKOiKuy0'.e[\\8OWq#qu9qK\\#K&SOueGSi&Cqu-.u&S#8CSmeGiuO-[OWSa&88y&iOG[&''eCum-yau-C['&GG\\[imyaOKC&8#q08mi.m[-\\aa-'W8W'4O.eK90[G8.-#-&-&\\8[4WG[umO8#OO-K[u8ae&4C'&-0&q[mGS\\0'SyG.Saei0\\0#SOeW4S[WK#4CuqC[[4Sy\\-S0.#0GqyW0['e.CO''9m.-qK-8.Su#&'eWe0#m'..m&.Ki#Wu'4.\\O8'#O8m#4-#iaO'Oq-CK4i&iaWqqySe8imC\\KCy[Cq0q\\0.eu8.iyya\\ymS4aW[CCq9W.ym.KCCa.imem[K#&.[8m0Wim-S[-#u'uC4qS#&8ya4&4&q'-S\\0#CGCq-'[m0uC[i0aCaqi#\\-y4-q#&&Wi0.4q-Cq8a0\\mGqm9m.mi-G&'GyKm8C4Kq&K#&4[#[qGy#S4e.Ge'Cy&u-e0mu&CaW4ymi#i&.[8yy'&u4'iq[CS-[-uGa.W8[8yaaGi4#\\O'eyKa8uK\\98a..WamS['C0KS0Om[GiC#4&['G[W.i0CuyKOC[i[e-WCS8'-aCyW8[-ay8''OC\\&a8i&0-GKK0.K88[uqGy#aCSq.ye'iOe.Gme9a.'4uyy#8KC0KaWS\\Wq'&q\\[\\[CmS0WiO.G\\KWy4uyWS0y&qOW4&-mqa&W#OyGuOG4a'8[8ySaC.4-0['4yGS0CG&8i\\G.Wqq'OC9G44SyWSSK0K\\WyWGu-yy[u-\\-Ce.\\0iu-CO04Syme.Ky\\#08WaquGKKi'44qaq0eq[iq8u'#a.\\.0#yu-4G#0-OuGW-meu#ai&0C";

        int[] dic = new int[256];

        byte[] bytes = str.getBytes();

        byte pre = bytes[0];
        for (int i = 1; i < bytes.length; i++) {
            dic[str.charAt(i)]++;
//
//            System.out.println(bytes[i] - pre);
//
//            pre = bytes[i];
        }



        int count = 0;
        for (int i = 0; i < dic.length; i++) {
            if (dic[i] > 0) {
                count++;
                System.out.println(i + " " + dic[i]);
            }
        }
        System.out.println(count);
    }
}
