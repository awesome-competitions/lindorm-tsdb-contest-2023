package com.alibaba.lindorm.contest.v2.tests;

public class TestAscii {

    public static void main(String[] args) {
//        String str = "i\\8K#0#0\\ySG.SSa[8.OmCiS\\\\mSyayK&aqa..Sm'Ca[[mG-0&-''''4q[muy#G0'KWmSa#4yKme48OmKuO'[4&iG[q#4[u\\44yu9q\\y#.mm8C.-K8iS[Gq[uy.-'&WK&u&4SSm'.WayWaO[OeC\\4C00&-8C&yim\\qqaeCO..uCKWaCSO-qS&-0maO-y#W''WSyeWuGy#9-'&'O840O'm8iyO''eO48yyeyy[444#e8WGq#mGqmu40[GyWCC8\\&8SG8qi'GO4&Gu.8[8y.\\KOe-meW.8O'#.\\[WK#u&.S[&a#S9aS'S4aC\\eu4CSyyiaGCeuq4e8iuee808u-0WeS-#G80yaOS.[4\\q0C\\8&\\y'W\\q#\\uKKWSeC\\aS[u'yyWaq0..'SS0y.yKGqKG.89yCm'4&m0--S.W0q[uam[eu-0K0S#O&-.mu'\\iaOq#0mKyS0[e-iOGSmia4SC4qyyy8ySa4S[ui#[8iS8yi4[iu8u8K[-aqy[u4&\\9K0aGK#4SyKW&0.y'-'0m'\\[C&'-OWu.\\'&WCe-\\.Kq\\i[-G'Kae0WmS.m0[8-eSO[iCaK\\mm#Gy.O[i[.yG\\y[a\\#eG..iuSySGa90[aC4iG-ye8[-C\\8i#O#O[aC\\.C-W#0'OOimCu.84\\e'qaO#-\\a\\[8Kuuq'quS8KCKSOqWSKCyW[.iyKeKOiKuy0'.e[\\8OWq#qu9qK\\#K&SOueGSi&Cqu-.u&S#8CSmeGiuO-[OWSa&88y&iOG[&''eCum-yau-C['&GG\\[imyaOKC&8#q08mi.m[-\\aa-'W8W'4O.eK90[G8.-#-&-&\\8[4WG[umO8#OO-K[u8ae&4C'&-0&q[mGS\\0'SyG.Saei0\\0#SOeW4S[WK#4CuqC[[4Sy\\-S0.#0GqyW0['e.CO''9m.-qK-8.Su#&'eWe0#m'..m&.Ki#Wu'4.\\O8'#O8m#4-#iaO'Oq-CK4i&iaWqqySe8imC\\KCy[Cq0q\\0.eu8.iyya\\ymS4aW[CCq9W.ym.KCCa.imem[K#&.[8m0Wim-S[-#u'uC4qS#&8ya4&4&q'-S\\0#CGCq-'[m0uC[i0aCaqi#\\-y4-q#&&Wi0.4q-Cq8a0\\mGqm9m.mi-G&'GyKm8C4Kq&K#&4[#[qGy#S4e.Ge'Cy&u-e0mu&CaW4ymi#i&.[8yy'&u4'iq[CS-[-uGa.W8[8yaaGi4#\\O'eyKa8uK\\98a..WamS['C0KS0Om[GiC#4&['G[W.i0CuyKOC[i[e-WCS8'-aCyW8[-ay8''OC\\&a8i&0-GKK0.K88[uqGy#aCSq.ye'iOe.Gme9a.'4uyy#8KC0KaWS\\Wq'&q\\[\\[CmS0WiO.G\\KWy4uyWS0y&qOW4&-mqa&W#OyGuOG4a'8[8ySaC.4-0['4yGS0CG&8i\\G.Wqq'OC9G44SyWSSK0K\\WyWGu-yy[u-\\-Ce.\\0iu-CO04Syme.Ky\\#08WaquGKKi'44qaq0eq[iq8u'#a.\\.0#yu-4G#0-OuGW-meu#ai&0C";

        String[] strs = {
                "kY/gQc;QE6Us]QU",
                "Ys]cc=;QM/IEYkk",
                "(=(EMgYQI(Qws/(",
                "EUggg2gw;o;EUo=",
                "!k/EMkMg!oc2kIo",
                "k(U2,Ag=,soQo!,",
                "=w%2gsQo2,wQo;A",
                "QcQ/;!E2!skY%6M",
                "MskkkI!ksY;YkIc",
                "U,c(c]UIAQ;(Y22",
                "UII=Us;=6Q2=;,o",
                "%!w=g6!o(6M(U6]",
                "Mw2=U!(M,]oo=EY",
                "2s!kIII;6=!sA!g",
                "(co6!2!I%QEc!,c",
                "!I,=(s22gA=Qg=s",
                "6Y2UY;,k262Qc/=",
                ";s%Qo662ookwc%k",
                "=I2!(]ww2s2;]gE",
                "!U,UsM=IgE6w/(=",
                "ck](Y,2s]E(Uc/2",
                ",2Mw]wwsA,,w%MY",
                "2oUIE,]2=I2%!(s",
                "/Q!6o2QEc!U2Uc]",
                "Q!,!swYE!Y]oc=M",
                "EwAg6=kQo!;U2UQ",
                "wMo%wc%koAI%/M!",
                "kYUk!cA=6MMUkYo",
                "(s22U(c,IAQgQsw",
                "=2%Y%U!Ec/2I(6s",
                "//sgc%M=!g=so;o",
                "2;,o(A]!Uo!,%!;",
                "],6IEc!YY=g6U,,",
                "6wUI;QQ;kM;2Y66",
                "6w]/Iw]Ec/26Q/6",
                "%/sg%%!2YEc]E(s",
                "YYg/IAA,(Io/=Ic",
                "(Io2(QEYoUw,]!,",
                "(QYg]MsQ(Mw,/6A",
                "6%Eo!%Y=c=%IE](",
                "=k=kEgAQ]A2g,MM",
                "!E!g;]A;UgQssgc",
                "oQQ,QcY2sU!2UYY",
                "Qg;k,gQwM=/sokA",
                "g(!,;gMsU;w,MI,",
                "6cIg2Ms;=%];(!,",
                "/kw(6/EoA,MwAY/",
                "M(2k6sI!A;I;%A6",
                "]g!6,%2;%!!!62s",
                "As,cUYkwMYgYM%6",
                "Y%o62=k=g(;U/!U",
                ";gsM=;;I%A6;s;I",
                "AoM/MM2(6%Usog,",
                "(26%!62k2c;E/,o",
                "ck/U!6]cU2/Io/]",
                "kIEko(2kY(E;;wo",
                ";gMIYIY(IEw=,c6",
                "%AQ;AI(;ockYos;",
                "kI6M2o%Y]U(=;gE",
                "%s!skM(]6cM]Uc/",
                ",c!,Eo,!6(6kkw2",
                "/QE!Q%g6/kMY2=Y",
                "6/wcAE!=/go%I6Q",
                "((Y%UUw/QA,kY]w",
                "UU!E2g,Y]w]o%!w",
                "kYo2MoEkcA(c6E]",
                "Q!]2IAgYU];=]o;",
                "gAEAsIck!;=!E=U",
                "I=]Q=Qs;,kco!6kY/gQc;QE6Us]QUEYYs]cc=;QM/IEYkk(=(EMgYQI(Qws/(EUggg2gw;o;EUo=!k/EMkMg!oc2kIok(U2,Ag=,soQo!,=w%2gsQo2,wQo;AQcQ/;!E2!skY%6MMskkkI!ksY;YkIcU,c(c]UIAQ;(Y22UII=Us;=6Q2=;,o%!w=g6!o(6M(U6]Mw2=U!(M,]oo=EY2s!kIII;6=!sA!g(co6!2!I%QEc!,c!I,=(s22gA=Qg=s6Y2UY;,k262Qc/=;s%Qo662ookwc%k=I2!(]ww2s2;]gE!U,UsM=IgE6w/(=ck](Y,2s]E(Uc/2,2Mw]wwsA,,w%MY2oUIE,]2=I2%!(s/Q!6o2QEc!U2Uc]Q!,!swYE!Y]oc=MEwAg6=kQo!;U2UQwMo%wc%koAI%/M!kYUk!cA=6MMUkYo(s22U(c,IAQgQsw=2%Y%U!Ec/2I(6s//sgc%M=!g=so;o2;,o(A]!Uo!,%!;],6IEc!YY=g6U,,6wUI;QQ;kM;2Y666w]/Iw]Ec/26Q/6%/sg%%!2YEc]E(sYYg/IAA,(Io/=Ic(Io2(QEYoUw,]!,(QYg]MsQ(Mw,/6A6%Eo!%Y=c=%IE](=k=kEgAQ]A2g,MM!E!g;]A;UgQssgcoQQ,QcY2sU!2UYYQg;k,gQwM=/sokAg(!,;gMsU;w,MI,6cIg2Ms;=%];(!,/kw(6/EoA,MwAY/M(2k6sI!A;I;%A6]g!6,%2;%!!!62sAs,cUYkwMYgYM%6Y%o62=k=g(;U/!U;gsM=;;I%A6;s;IAoM/MM2(6%Usog,(26%!62k2c;E/,ock/U!6]cU2/Io/]kIEko(2kY(E;;wo;gMIYIY(IEw=,c6%AQ;AI(;ockYos;kI6M2o%Y]U(=;gE%s!skM(]6cM]Uc/,c!,Eo,!6(6kkw2/QE!Q%g6/kMY2=Y6/wcAE!=/go%I6Q((Y%UUw/QA,kY]wUU!E2g,Y]w]o%!wkYo2MoEkcA(c6E]Q!]2IAgYU];=]o;gAEAsIck!;=!E=U"
        };

        for (String str: strs){
            printDic(str.getBytes());
            System.out.println();
        }



    }

    public static void printDic(byte[] bs){
        int[] dic = new int[256];
        for (int i = 0; i < bs.length; i++) {
            dic[bs[i]]++;
        }

        for (int i = 0; i < dic.length; i++) {
            if (dic[i] > 0) {
                System.out.print(((char)i) + " ");
            }
        }
    }
}
