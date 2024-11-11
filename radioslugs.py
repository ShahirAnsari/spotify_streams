from datetime import datetime
import pandas as pd
import concurrent.futures
import requests
from datetime import datetime
import csv
import time



# params = {
#     "startDate" : "2024-03-06",
#     "endDate" : "2024-05-28"
# }
username = "mbairathi@gmail.com"
password = "D@ta_access1"

headers = {
        "x-app-id": "UCL_19889CC5",
        "x-api-key": "582828be01568226"
    }



params = {#"radioSlugs" : "bbc-2,capital-fm",
          "radioSlugs" : "2dayfm,2ser,2uus-ws-fm-101-7-pure-gold,3fm,3ptv-smooth-fm-91-5,40-principales,47-fm,4bfm-97-3-fm,4mmm-triple-m,4rgd,4sea-90-9-sea-fm,5add-mix-102-3-1,5fm,666,6now-96-1-fm,89-89-1fm,947-highveld-stereo,energia-97-fm,absolute-classic-rock,absolute-radio-70s,absolute-radio-90s,absolute-80s,absolute-radio,activ-radio,afk-max,africa-ndeg1,alfa-91-3fm,radio-allgau-hit-1,alouette,alpes-1,alpha-fm,amazing-radio,antena-1,antenne-steiermark,antenne-1-baden-wurttemberg-1,antenne-bayern-1,antenne-thuringen-1,arl-fm,arrow-classic-rock,aspen-102-3,atlantida-fm,averne,baden-fm,bayern-1,bayern-2,bayern-3,bbc-1,bbc-1-xtra,bbc-2,bbc-3,bbc-4,bbc-5,bbc-5-xtra,bbc-6,bb-radio,beat-100-9fm,beaub-fm,bel-rtl,bergerac-95,beur-fm,big-fm,blackbox,bnr,bonnfm,br-puls,bremen-eins,klassik-radio,bruzz,cadena-dial,cadenas-100,canal-b,canal-fiesta,canal-fm,canale-italia,cannes-radio,capital-fm,capital-xtra,100-radio-1,cerise-fm,cfmy,cfou,champagne-fm,chante-france,cherie-fm,chrx,chum,cibl,ciccioriccio,cidr,cism,ckbc,ckfm,ckoi,ckrl,classic-21,coast,collines-la-radio,company,contact,contact-fm,radio-cote-damour,d99-98-9fm,dasding,deejay,delta-fm,delta-radio,dh-radio,dici-radio,die-neue-107-7,direct-fm,discoradio,echo-fm,radio-ecn,egofm,energie,energy-zurich,energy98,equinoxe,europa-fm,europa-plus,europe-1,evasion,xhma-fm,fbi,fc-radio-lessentiel,radio-fg,fip,flash-fm,flava,flor-fm,fluxfm,fm4,forum,france-maghreb-2,france-bleu,france-inter,frequence-grands-lacs,frequence-plus,fritz,fun-radio-bl,fun-radio,gaydio,generations-idf,georgefm,global,gold-104-3,gong-96-3,gozadera-fm,grafhit,grand-sud-fm,heart-fm,heart-fm-1,heat,hit-fm,hit103-1,hit105,hit107,hit92-9,hits-radio,hit-west,horizon,hot-radio,hr3,hr4-hessen,ibero-90-9fm,impact-fm,the-independent-fm,inside,iradio-fm-bandung,jacaranda,jaime-radio,jam-fm,jazz-radio,jedynka,jet-fm,joe-fm,jordanne-fm,jovem-pan,jukebox,k-fm,k6-fm,kafa,kalx-90-7-fm,kbfb,kbpi,kbxx,kcrw,los-40-urban-kebuena,kegl,kerrang,kexp,kglt,khks,kiis-101,kiis-1065,kiis,kioz,kiss-fm,kiss-fm-1,kiss-fm-2,kiss-fresh,kiss-fm-toulon-marseille-1,kiss-fm-3,kisw,kit-fm,kkdo,klif,kmel,kmvq,knrk,kpsu-98-1-fm,kpwr,krbq,krml,kronenhit-electric,kroq,krui,krzq,ksoc,kunm,kupd,kuws,kwss,kx-radio,kysr,leko-des-garrigues,lunico,la-fm,la-fresca-fm,la-radio-plus,la-x-103-9-fm,landeswelle,latina,latitude,lbc-radio,littoral-fm,lor-fm,los-40,los-40-principales,lyon-1ere,m2o,magic,magnum-la-radio,mai-fm,marcha-fm,maritima,marte-2,maxima-106-7fm,los-40-dance-maxima-fm,mbs,mdr-sputnik,mdr1,mdr,megastar,melodie,metro-95-1,metropolitana-98-5-fm,metropolys,mint,mistral-fm,mix-98-2,mix-fm,mix-fm-106-3fm,mixx-fm,mona-fm,montagne-fm,more-fm,motion,mouv,frequence-mutine,myrock,n-radio,nb-radiotreff,ndr2,nervion,newstalk-zb,n-joy,nostalgie-vlaanderen-1,nostalgie-wallonie-1,nova-1,nova-100,nova-106-9,nova-919,nova-93-7,nova-96-9,nova-2,npo-radio-1,nrj,nrj-wallonia,nrj-brandenburg,nrj-bremen-1,nrj-hamburg-1,nrj-munchen-1,nrj-nuremberg,nrj-sachsen-1,nrj-baden-wurttemberg,nrk-p1,nrk-p3,nts,o3,oceane-fm,ods-radio,oui-fm,oxygene-radio,oxygene-radio-hit-dance,oz-radio-1,p1,p2,p2-1,p3,p3-1,p4,p4-1,p5,p6-beat,p8-jazz,paramadu,pbs,pfm,phoenix,planet-radio-1,playstudio,plein-air,primitive,prun,prysm-radio-1,radio-psr,pure-fm,qmusic-bel,r101,radio-1,radio-2-be,radio-2-nl,radio-538,radio-alfa,radio-bonheur,radio-camargue,radio-caroline,radio-decibel,radio-dimensione-suono,radio-dio,radio-dreyeckland,radio-emotion,radio-en-construction,radio-eska,radio-grenouille,radio-hauraki,radio-intensite,radio-isa,radio-jade,radio-kiss-kiss-italia,radio-menergy,radio-meuh,radio-mont-blanc,radio-numero-1,radio-okerwelle,radio-ole,radio-one,radio-orient,radio-orte,radio-scoop,radio-star,radio-studio-1,radio-vfm,radio-zet,radio105,radio-21,radio-6,radio-7,radio-8,radio-arabella,radio-berlin,radio-bob,radio-brocken,radiodoblenueve,radio-espace,radio-ffn,radio-flash,radio-monaco,radio-neo,radionica,radiorecord,radio-salu,raidue,raiuno,rcv,rdl-radio,reactor-105-7fm,radio-regenbogen,reprezent-radio,rete-due,rete-tre,rete-uno,rfi,rfm,rhema,rinse-fm,riresetchansons,radio-rmb,rmc,rmf-fm,rmf-maxxx,rmx-100-3fm,rnz-national,rockland-radio,rosa,rouge-fm,rouge-fm-1,rpr1,rri-pro-1,rri-pro-2,94-3-rs2,r-sh-radio,rtbf-international,rte-2xm,rtl,rtl-102-5,104-6-rtl,rtl2,rtl-radio,rtr,rts1,rts-couleur-3,rts-fm-montpellier,berliner-rundfunk-91-4,rvm,rythmefm,sabbia,radio-saw,sky-radio,skyrock,slam,smooth-fm-95-3,smooth-fm,spazio-blu,105-5-spreeradio,sr1-europawelle,srf-musikwelle,srf2,srf3,wstr,star-fm-97-9,star-radio,studio-brussel,suby,sud-radio,sud-radio-1,sunshine-live,surco,sweet-fm,swr3,tendance-ouest,the-breeze,the-edge,the-hits,the-rock,the-sound,top-radio,top40,top-fm,top-music,transamerica-100-1fm,trax-fm,triple-r,triple-j,trojka,tropiques-fm,tsf-jazz,tsugi,87-8-ucfm,uni-radio,unserding,utv-pulse-1,utv-the-wave,veronica,vibration,vinci-autoroute-fm,virgin,virginuk,vivavcite-bru,voltage,vox-fm,w-radio-96-9fm,wasu-90-5-fm,wbbm,wbqt,wbtj,wckx,wcpt,1live,wdr4,wfmu,wfuz,wgpr,whhl,whsn,whtz,wicb-91-7-fm,wiyy,wjmn,wjrr,wkdu-91-7-fm,wknc-88-1-fm,wknd,wkqx,wksc,wktu,wqlz,wmmr,worldwide-fm,wowi,wpgc,wpow,wqht,wque,wrek-91-1-fm,wrff,wrif,wrrc,wrsv,wrur,wwdc,wxtb,wzmx,radio-x-xfm,yfm,you-fm,zm,1-pure-edm,1cbr-mix-106-3,2br,2gb,2kko-ko-fm,2mac,2one-the-edge-96-one,2uul,2win,2wsk,2wzd,2xxx-hit-106-9-fm-newcastle,3aw,3bay,3bba,3bdg,3cat,4ca,4ggg-4gld,4gr,4hot,4mcy,4mky,4mix,4rgc,4rgk,4rgm,4rok,4see,4sss,4to-triple-m-townsville,5dn-cruise-1323,6bun,6cst,6ix,7hho,8hot,92-7-big-fm,98-fm-1,amor,antena-2,antena-3,antipode,antyradio,ascendance-radio,beat-102-103,bens-radio,brava-radio,central-fm,cherie-fm-1,cidade-fm,classic-hits-4fm,3yfm-coast-fm,cool-fm,double-j,downtown-radio,educadora,fire-radio,fm104,forth-1,free-radio-fm,free-radio-fm-3,frisky-radio,gem-106,genesis,hallam-fm,hard-rock-fm,hit-104-7,hits-106-1fm,hot-tomato,inspiration-fm,iradio-2,jazz-fm,kbff-fm,kdwb-fm,key-103,khfi-fm,khts-fm,khtt-fm,kiso-fm,kiss-fm-4,kisstory,kjyo-fm,kkmg-fm,kkrz-fm,kmfm,kmxv-fm,knhc,kpwk-fm,krbe-fm,kslz-fm,ktbt-fm,ktfm-fm,kudd-fm,kudl-fm,kxxm-fm,kzht-fm,kzzp-fm,la-99-1,la-buena-onda,lora,magic-1,mega-hits-fm,metro-fm,mi-soul,mix-94-5,mix-fm-1,moray-firth-radio,most-fm,naija-fm,nexus-radio,northsound-1,ok-radio,p4-2,pirate-fm,planet-rock,prambors,pride-radio,pulse-87-ny,q102,rabe,radio-afera,radio-birikina,radio-bruno,radio-chico,radio-cite,radio-city,radio-comercial,radio-cooperativa,radio-danz,radio-disney-1,radio-disney-2,radio-futuro,radio-globo,radio-ibiza,radio-kaiseregg,radio-la-zona,radio-mirchi-98-3-fm,radio-moda,radio-norge,radio-nueva-q,radio-number-one,radio-oxigenio,radio-piterpan,radio-quartz,radio-radar,radio-renascenca,radio-rock,radio-sonar,radio-x-1,radionorba,rasa,ray-power,rds,rfm-1,ritmo-romantica,rock-fm,rte-2fm,rte-radio-1,signal-one,spin-1038,studio-piu,sulamerica-paradiso,sun-fm,talkradio,tay-fm,tfm,the-hits-1,today-fm,topp-40,triple-m-sydney,triple-m-melbourne,triple-m-adelaide,veronica-my-radio,viking-fm,waks-fm,wazobia-fm,wdcg-fm,wdjx-fm,wezb-fm,wflc,wflz,wfmf-fm,whbq-fm,whqc-fm,whyi,wioq-fm,wkqi-fm,wksl-fm,wldi-fm,wnci-fm,wods,wrnw-fm,wrov-hd2-alt-project,wrvw-fm,wwkl-fm,wwmx-fm,wwpr-fm-power-105-1,wwpw-fm,wwst-fm,wwxm-fm,wxks-fm,wxlk-fm,wxss-fm,wxxl,wxxx-fm,wyks-fm,wyoy-fm,wyrg-fm,wzee-fm,wzfl,wzft-fm,wznf,wzok,wzyp,xejp,xeoye-oye-89-7-fm,xhcel,xhqro-fm-radar-107-5,xherz,xhfg,xhin,xhitz,xhjc-exa-91-5-fm,xhls,xhltn,xhmg,xhmls,xhnu,xhob,xhsol,xhtab,xhto,xhtp,xhts,xhuad,xhzm,wdr-3,radio-3,nostalgie,galaxie-radio,sfera-102-2-fm,lrt-radijas,radiocentras,ndr-90-3,swr-4-bw-1,radio-mti,vlr,swr-1-rp,bbc-radio-lancashire,bbc-radio-manchester,radio-100,hr-2,radio-paradiso,radio-cottbus-94-5,est-fm,hr-1,antenne-landau,radio-primavera,antenne-zweibrucken,mellow-magic,donau-3-fm,fubar-radio,ndr-radio-kultur,actualitati,hitradio-rt1-nordschwaben,bayernwelle-sudost,bbc-asian-network,reform-radio,kiss-fm-92-2,hitradio-rt1-sudschwaben,bbc-radio-wales,bbc-radio-cymru,bbc-radio-solent,hoxton-fm,bbc-radio-leeds,radio-24-pop,die-neue-welle,virage-radio,m-1,bbc-essex,mdr-kultur,radio-klinikfunk,radio-isw,antenne-kaiserslautern,p5-hits,toulouse-fm,b-5-aktuell,heart-yorkshire,argovia-pop,r-sasachsen,hh-zwei,hit-radio-n1,petofi,star-fm-nurnberg,radio-galaxy,apollo-radio,xs-manchester,yle-radio-1,radio-alpenwelle,deutschlandfunk,p6-rock,radio-swiss-pop,nrk-p1-1,deutschlandradio-kultur,radio-mainwelle,nation-radio-cardiff,radio-flaixbac,ndr-1-niedersachsen,bbc-radio-bristol,vogtland-radio,radio-euroherz,mix-megapol,kiss-fm-104-5,hitradio-rt1,bbc-surrey,radio-marca-barcelona,hit-radio-ohr,radio-canal-3,clyde-1,rmn,city-radio-trier,ndr-1-radio-mv,fun-kids-london-1,bbc-radio-merseyside,totem,radio-bamberg,sr-2-kulturradio-1,pie-radio-uk,radio-zu,radio-plassenburg-101-6,rock-antenne,bylgjan,radio-lietus,ndr-info,radio-oberland,talksport-london,nrj-sweden,radio-awn,radio-ton,radio-trausnitz,antenne-pirmasens,bbc-radio-ulster,flaix-fm,95-7-fm,radio-seefunk-rsf-1,premier-christian-radio,radio-suomipop,radio-nordseewelle,bbc-radio-berkshire,derti-98-6-fm,the-beat-103-6fm,radio-hannover-87-6-r,radio-horeb,love-80s-radio-manchester,radio-teddy,mnm-hits,resonance-extra,bbc-sussex,bbc-three-counties-radio,ras-2,ras-1,antenne-frankfurt,rock-antenne-hamburg,radio-schlagerparadies,fusion-fm,radio-ramasuri,lincs-fm,eldoradio-alternative,north-manchester-fm,rockklassiker,kulturradio-rbb,radio-soft,unserradio-passau-1,radio-essex,lyca-radio-1458,radio-subasio,antenne-koblenz-98-0,antenne-bad-kreuznach,panjab-radio,tonic-radio,skala,hitradio-rtl-sachsen-1,bbc-radio-scotland,ostseewelle-hit-radio-mecklenburg-vorpommern,profm-black,wit,sea-fm,westside-radio,gold-london,classic-fm,magic-soul,bbc-london,rbb-radioeins,rock-fm-1,rac-105,rock-pop,radio-uno,pop-radio,mega,fm-o-dia,piata-fm-salvador,nova-brasil-fm-sao-paulo-1,bh-fm,costa-verde-fm,tropical-fm-1,jb-fm,itapoan-fm-1,beat-98-1,transcontinental-fm-1,blink-102-fm-campo-grande-1,vida-fm-fortaleza,lideranca-fm,saudades-fm,radio-record,melodia,cidade-fm-rio-de-janeiro-1,cidade-98-fm,capital-fm-102-7,bahia-fm-1,concierto-punta-95-1-fm,azul-fm-101-9,cw33-la-nueva-radio-florida,cjad,chqr,ckno-fm,cicx-fm,cbw,cbcl-fm,cbu,cjmf-fm,choi-fm,ckqb-fm,cjob,ckry-fm,chkx-fm,cbv-fm-1,chym-fm,cfjb-fm,ckkq-fm,cfqx-fm,chez-fm,ched,cbo-fm,cfra,chfi-fm,cknw,cbr,cjkx-fm,1075-kzl,vivalavoce,the-beat-midlothian,92q-jams-baltimore,whtp-fm,wzne-fm,klly-fm,khth-fm,wstw-fm,wpwx-fm,kfrr-fm,kksw-fm,kkxx-fm,kddb-fm,wixx-fm,wil-fm,krxp-fm,wpat-fm,whur-fm,wtmx-fm,wcoo-fm,wcpy-fm,wndv-fm,wbez-fm,wdkx-fm,wxrv-fm,wjmz-f2,kdgs-fm,wfly-fm,koit-fm,wncy-fm,kioa-f2,wuvt,wvyb-fm,kwyd-fm,wogk-fm,wxpn-fm,kspw-fm,wlzx-fm,wwj-am,kcaq-fm,kyw-am,wtdy-fm,wjmh-fm,kkwf-fm,kzok-fm,wbeb-fm,wzzk-fm,wqok-fm,wxrt-fm,kfco-fm,kcbs-am,kzmg-fm,kblx-fm,kiro-fm,wbzz-fm,kcfr-fm,kmox-am,kqmv-fm,kdkb-fm,whta-fm,wjfk-fm,kkpn-fm,wmmj-fm,wrmf-fm,wycd-fm,wqlq-fm,weqx-fm,knci-fm,wube-fm,wkhk-fm,wamu-fm,ksop-fm,wdrv-fm,wkys-fm,wley-fm,whzt-fm,wctk-fm,wgts-fm,kzfm-fm,wmme-fm,wwyy-fm,wtic-fm,wayv-fm,kseq-fm,kllc-fm,wzgc-fm,waji-f2,wbhj-fm,kqch-fm,ktts-fm,kkuu-fm,wshe-fm,wdzh-fm,wenz-fm,wamj-fm,wezn-fm,wcgq-fm,kisv-fm,wmrq-fm,wins-am,klyy-fm,2ski-snow-fm-97-7,3bo-triple-m-bendigo-93-5,7xxx-triple-m-hobart-107-3,joy-94-9,2bdr-triple-m-the-border-105-7,3sr-triple-m-goulburn-valley-95-3,2aay-hit-the-border-104-9,3nnn-edge-fm-102-1,3shi-mixx-fm-107-7,7ttt-hit-hobart-100-9,2ca,rthk-radio-3,rthk-1,radio-city-91-1,kol-rega-91-5-fm,beat-radio,fm-cocolo,virgin-radio-89-5,best-fm,sinar-fm,yes-fm-manilla,love-radio-manilla,91-5-win-radio-manilla,class-95,capital-95-8fm,987,love-972,yes-933,arirang-radio,tbs-fm,mcot-met-107-fm,cool-fahrenheit,radyo-fenomen,joy-turk-fm,number-one-turk,power-turk-fm,super-fm,wxyt-fm-the-ticket-detroit-sports-97-1-fm,wskq-fm-la-mega-new-york-97-9-fm,wzlx-fm-bostons-classic-rock-100-7-fm,3wm-1089-am,3yb-94-5-fm-1,4kz,5au-1242-am,wor-am-710-wor-the-voice-of-new-york,fm-del-hum,radio-charivari-munchen,kqbl-f3-boise-96-5-fm,wrox-fm-96x-norfolk,wpia-fm-kiss-fm-peoria-98-5,kupl-fm-the-bull-portland-98-7-fm,kljy-fm-joy-saint-louis-99-1-fm,radio-nrw-r,bbc-radio-kent,bbc-radio-sheffield,bbc-birmingham,bbc-world-service,cfwmfm-bob-fm-99-9,bremen-vier-1,bremen-2-former-nordwestradio,cjbx-fm-bx93,cbx-cbx-edmonton,radio-charivari-nurnberg,radio-charivari-regensburg,chris-country-radio,chtzfm,classic-rock-radio,italo-disco,cool-fm-1,downtown-country,ckmm-fm-energie-montreal-94-3,cing-fm-fresh-95-3-fm-1,cfpl-fm-londons-best-rock-fm-96,wxyy-fm-g100-1-fm-savannah,radio-gong-fm-regensburg,wtok-fm-hot-san-juan-puerto-rico-102-5-fm,wjfx-fm-online-fort-wayne-hot-107-9-fm,hot-fm,ckbe-fm,kruf-fm-k945-shreveport-94-5-fm,cklh-fm-k-lite-102-9,kkda-fm-myk-dallas-hiphop-104-5-fm,kalv-fm-live-phoenix-101-5-fm,kbzt-fm-alt-san-diego-94-9-fm,kcfm,kfrg-fm-k-frog-san-bernardino-95-1-fm,wzkx-fm-kicker108-gulfport-107-9-fm,kiss-fm-5,ckks-fm,kjkk-fm-jack-fm-dallas-100-3,ckmb-fm-kool-fm-107-5,kseg-fm-the-eagle-sacramento-96-9-fm,ksfm-fm-ksfm-sacramento-102-5-fm,kson-fm-kson-san-diego-103-7-fm,kswd-fm-the-sound-seattle-94-1-fm,kvil-fm-alt-dallas-103-7,kyky-fm-y-saint-louis-98-1-fm,krzz-fm-la-raza-san-francisco-93-3-fm,lfm,m-radio,magic-chilled,cjmj-fm-majic-100-3,mapo-fm,mas-fm-101-3,jump,mix-fm-104-4,nrj-4,radio-1-1,kphw-fm-power-hawai-104-3-fm,kmck-fm-power-arkansas-105-7fm,kopw-fm-omahas-power-106-9-fm,2eee-power-104-3-fm,power-hit-radio,kwpz-fm-praise-106-5,wkrq-fm-q102-wkrq-cincinnati,wdjq-fm-q92-radio,chqm-fm-qm-fm-103-5,radio-osnabruck-98-2,radio-8-1,radio-aktiv,radio-charivari-rosenheim,radio-eins-coburg,radio-fantasy,radio-gong-106-9-wurzburg,radio-gong-97-1-nurnberg,radio-hamburg,radio-lac,radio-nova,radio-paloma,radio-inconfidentes,rix-fm,kfma-fm-rock-tuscon-102-1-fm,cite-fm-rouge-fm-107-3,cimf-fm-rouge-94-9,das-neue-rsa-radio,kpat-fm-the-beat-san-maria-95-7-fm,kblz-fm-the-blaze-winona-102-7-fm,wbtz-fm-the-buzz-99-9-fm,the-voice,kxna-fm-the-x-arkansas-104-9-fm,106-4-top-fm-1,kuuu-fm-u92-salt-lake-city-92-5-fm,cjfm-fm-virgin-radio-96,know-wave-radio,wcbs-am-wcbs-newsradio-new-york-880-am,wfbc-fm-b93-7-fm-greenville-pop-music,wip-fm-sportsradio-philadelphia-94-1fm,wkse-fm-kiss-niagara-falls-98-5fm,wlnk-fm-the-link-charlotte-107-9-fm,wlw-am-newsradio-700-wlw-1,wnyl-fm-alt-new-york-92-3-fm,wogl-fm-wogl-philadelphia-98-1-fm,wpxy-fm-98-pxy-rochester-97-9-fm,wscr-am-the-score-chicago-670-am,wusn-fm-us99-radio-chicago-99-5-fm,wwbx-fm-mix-104-1-fm,kxrk-fm-x96-utahs-original-alternative-96-3-fm,klyv-fm-y105-music-dubuque-pop-radio,disney,urban-hit,2vly-power-fm-98-1,3ccs-mixx-fm-92-7,3el,3gv-gold-1242-am,3ml,4rgr-power-100,5mu-1125-am,7ad-900-am,7ddd-sea-fm-107-7,7exx-chilli-fm-90-1,7la-lafm-89-3,7sea-sea-fm-101-7,ishq-fm-104-8,rtl-radio-letzebuerg,lessentiel-radio,radio-100-7,rtl-deutschlands-hit-radio,alt-nation,octane,the-pulse-radio,pop-2k,shade-45,siriusxm-chill,hits-1,venus,the-spectrum,xmu,the-loft,eldoradio-chill-channel,radio-verona,new-style-radio,radio-cardiff,wcr-fm,ujima-98-fm,bcfm,delta-90-3,geel-fm,urgent-fm,evropa-2,radio-1-2,cjsr,byte-fm,dish-fm,bln-fm,republic-radio,highland-radio,zip-fm-1,radio-m-1,zip-fm-2,antenna-5,kiss-fm-algarve,radio-deep-romania,ibiza-global-radio,val-202,radio-as-fm,dinamo-fm,kiss-fm-ukraine,subfm,unity-fm,kane-fm,select,wnyu-fm,kxci,power-fm,virgin-radio-1,joy-fm,wbls-fm,krrl-fm-real-92-3,radio-tirana-2,rtva-andorra-musica,bh-radio-1,bnr-horizont,hr2-drugi-program,cybc-trito,czech-radio-wave,eesti-raadio-2,yle-x3m,pieci-5,radio-naba-6,lrt-opus,npo-funx-nl,polskie-radio-czworka,radio-beograd-1,rtv-radio-fm,ur2-radio-promin,2st,2ay-1494-am,3gg-531-am,4aa-1026-am,5cs-1044-am,5auu-magic-105-9-fm,3wwm-mixx-fm-101-3,4rgt-star-106-3,4cc-zinc-927-am,4nnn-zinc-96-1,1005-das-hitradio,chom-fm-chom-97-7,89-0-rtl,radio-f,radio-hbw-harz-borde-welle,radioprimaton,dwls-barangay-ls-97-1,dorojnoe-radio,dilse-radio-1035am,krrf-fm-106-3-spin-fm,wvkl-fm-95-7-fm-r-b-norfolk-urban-music,wkrz-fm-98-5-krz-fm-wilkes-barre,keze-fm-hot-96-9fm-spokane,womx-fm-mix-105-1fm-orlando-pop-music,kmyz-fm-z104-5fm-tulsa,wbz-am-wbz-news-radio-103-0fm,weei-fm-weei-93-7fm,indie-xl,kink,npo-radio-5,radio-disney-chile,radio-disney-peru,eldoradio-live,radyo-s,kral-world,blu-radio,radio-city-1,radio-klara,tarmac,max-fm,kemet,eazy-fm,mellow-97-5,hitz-fm-1,ardan-radio,kisi-fm,pilar-radio,dj-fm-1,gajah-mada,pinguin-fm,sushi,kiss-fm-6,2sm,macquarie-sports-radio-954,sen,sport927,4bc,5aa,6pr,bxfm-104-3-fm,bpm-1,hip-hop-nation,the-coffee-house,the-blend,underground-garage,radio-margaritaville,classic-vinyl,lithium,liquid-metal,the-heat,siriusxm-fly,no-shoes-radio,outlaw-country,faction-punk,maximum-fm,kanal-k,kz-radio-israel,radio-campus-lille,radio-dijon-campus,radio-freiburg,radio-stadtfilter-96-3-fm,toxic-fm,radio-vostok,srf-1-basel-baselland,srf-virus,radio-kampus-fm,rdp-acores-antena-1,radio-atlantida,radio-horizonte,radio-canal-fm,portuguese-radio,radio-benedita-fm-88-1,radio-guadiana,pef-posto-emissor-do-funchal-canal-1,radio-arc-en-ciel,radio-antenne-portugaise,rtp-antena-1,cidade-hip-hop,radio-amalia,radio-lagoa,sbsr-fm-radio-super-bock-super-rock,vodafone-fm,radio-94-fm,smooth-fm-1,radio-marginal,qmusic-1,fm-802,nhk-r2,kiss92,fmriberu-fm-riviere,npo-3fm-alternative-1,sublime-radio,pinguin-radio,onda-cero,radio-la-kalle,la-karibena,capital-dance,radio-activa-89-9fm,like-fm,icrt,power-98-fm,radio-metro,hit-fm-107-4-hit-fm,dfm-radio-101-2-fm-dfm-radio,hitz-955,capital-fm-1,play-fm,panamericana-radio,radiomar-106-3-fm,100-nl,icat-fm-1,8k,rai-radio-3-1,radio-z-1,95bfm-1,vina-fm,radioaktiva,la-x,nippon-cultural-broadcasting,soho-radio,brum-radio,platform-b,radio-reverb,radio-willy,citi-fm,joy-fm-1,starr-fm,hitz-fm-4,3-fm,class-fm-1,peace-fm,adom-fm,the-beat-99-9-fm,soundcity-radio,rhythm-93-7-fm,noods-radio,swu-fm,nrg-radio,hot-96,capital-fm-2,radio-jambo,classic-105-fm,groot-fm-90-5,bok-radio,overvaal-stereo,pretoria-fm,bosveld-stereo,radio-laeveld,j-wave,fm-yokohama-1,nack-5,bay-fm-1,inter-fm,nippon-broadcasting-system-inc-nippon-fang-song,deutschlandfunk-nova,difusora-fm-97-1,kiss-fm-kobe,fm-aichi,tokyo-fm-1,love-fm,abc-radio,tbs-radio-inc,north-wave,radio-102,radio-metro-oslo-akershus,jaerradioen,orf-o1-inforadio,orf-o2-radio-salzburg,life-radio,88-6-wien,orf-o2-radio-vorarlberg,orf-o2-radio-steiermark,orf-o2-radio-wien,orf-radio-burgenland,orf-o2-radio-niederosterreich,mundo-livre-fm-curitiba,rock-101,cosmo-wdr,bremen-next,radio-milwaukee-wyms,deutsche-welle,radio-zwickau,radio-leipzig,radio-koln,radio-galaxy-landshut,radio-galaxy-ingolstadt,radio-galaxy-aschaffenburg,radio-dresden,radio-chemnitz,radio-bonn-rhein-sieg,hitradio-rt1-neuburg-schrobenhausen,hit-radio-ffh-1,antenne-dusseldorf-1,radio-91-2-lokalfunk-dortmund,colourful-radio,solar-radio,smooth-98-1-fm,foundation-fm,croydon-fm,dash-mixtape,greatest-hits-radio,radio-monte-carlo,radio-formula-segunda-cadena,coles-radio,radio-monte-carlo-2,radio-studio-93,radio-bussola,controradio-firenze,delite-radio,frequence-k,radio-phenix,radio-campus-bruxelles,radio-campus-bordeaux-1,radio-campus-besancon,radio-campus-angers-1,radio-campus-paris-1,enter-zagreb,galgalatz,radio-monte-carlo-st-petersburg,radio-campus-clermont-ferrand,radio-campus-brest,radio-san-marino-fm-102-7,kosmos-radio-93-6-107-0,georgian-public-broadcasting-radio-one,radio-moldova,mrt-radio-2,wdr-2,swr-aktuell,mdr-klassik,klassik-radio-1,one-fm-91-3,metro-fm-1,sbs-leobeu-fm-sbs-ladio,kbs-1-radio-main,mbc-fm,gold-90-5-fm,88-3-jia-fm,hao-fm-96-3,ufm-1003,prvi-karlovacki-wow-hits,hr-radio-dubrovnik,radio-101,hr-radio-sljeme,hrvatski-katolicki-radio,radio-kaj,antena-zagreb,radio-donji-miholjac,ukhozi-fm,east-coast-radio,goodhope-fm,ofm,mfm-92-6,mix-fm-93-8,vow-voice-of-wits-88-1-fm,mutha-fm,uct-radio-104-5-fm,radio-nfm-nuus,retro-radio-beat,hit-radio-maroc,npo-sterren,radio-rijnmond,omroep-brabant,radio-noord,linda-radio,radio-4,radio-mallorca,radio-corazon,studio92,slack-city,1-fm-samba-hits-brazil,band-fm,band-fm-96-1-fm,jovem-pan-fm-sao-paulo,rhone-fm,radio-ticino,virgin-hits,welle-1-salzburg,magic-89-9,monster-rx-93-1,z-99-9-fm,mad-radio,paradise-fm,sbc-radyo-sesel,omrop-fryslan,nh-radio,omroep-west,omroep-zeeland,sg1-radio,gorgeous-fm,all-fm,k107-fm,trans-radio-uk,dash-hip-hop-x,wiow-102-3-wow-radio,cjum-fm-umfm,blur-fm,iskelma,hitmix,yle-radio-suomi,mix-fm-rio,radio-cdl-102-9-fm,radio-clube-fm-brasilia,radio-so-flashack,rock-fm-brasil,transamerica-hits-belo-horizonte,nova-brasil-89-7-sp,radio-80-fm,radio-saudade-fm-100-7,radio-city-3,nrj-1,radio-nostalgia,radio-mars,match-fm,fm-like-97-1,lux-fm-padio-luks-lviv,lux-fm-103-1-padio-luks,hit-fm-hit-fm,ukhozi-fm-1,radio-raheem,kan88,kif-radio,radio-slovensko,boom-radio,97-9-home-radio,106-7-energy-fm,wish-107-5,mellow-94-7,96-3-easy-rock,radio-rock-1,female-radio-97-9-fm,rdi-97-1-fm,ptpn-radio-99-60-fm,99-1-delta-fm,c-lab,raje,france-culture,vivre-fm,sonora-lampung-fm-indonesia,chaine-iii,nevers-fm,radio-disney-3,gull-bylgjan,wcbs-fm,wave-radio,myhits,power-fm-1,loop,bbc-radio-cornwall,radio-swh-rock,radio-swh,radio-bielsko,radio-victoria,radio-rsc-88-6-fm,sun,rythmos-94-9,easy-97-2,star-fm,virgin-radio-ckfm-fm,ckis-fm-kiss-92-5,radio-pilatus,radio-bern-1,radio-basilisk,radio-rtn,radio-chablais,radio-alpa,radio-mega,sol-fm-1,radio-fmr,radio-pulsar,radio-beton,original-106,nrj-energy-bern,radio-zurisee,tsf-radio-noticias,nrj-radio-energy,antenne-vorarlberg,rmc-1,my105-dance,radio-carmarthenshire,h2o-annecy,radio-arabella-1,radio-topp-40-sverige,n-joy-1,radio-fresh-100-3,89-7-bay,vibe-fm,retro-fm,avto-radio,latidos-93-7,3fach,itch-fm,sr-3-saarlandwelle-1,metro-fm-2,itapema-fm,la-bestia,meuse-fm,go-country,radio-zenith-messina,4zzz-fm-4-triple-z,562live-long-beach,nrj-reunion,exo-fm,nrj-guyane,sunny-fm,pluzz-fm,kan-gimel,eco-99-fm,nrj-antilles,konbini-radio,resonance-fm,rdwa,krth-k-earth-101-fm,turbo-98-3,rtbf-jam,city-fm,premier-gospel,linq-fm,zfm,538-dance-department,mixx-fm-martinique,virgin-radio-uae,neat-fm,100fm,glz,soogood-radio,radio-funx-fissa,radio-campus-montpellier,kronehit-105-8,hotmixradio-dance,asaase-radio,amazing-radio-usa,luv-107-1,radio-1-4,radio-neptune,ab-95-fm,radio-top,radio-munot,rdyw-lb-hmdynh,antenne-niedersachsen,breeze-fm,kasapa-fm,happy-98-9-fm,onua-95-1-fm,star-fm-1,wrlt-lightning-100-1-fm,radio-ara,radio-latina,wktt-live-97-5-fm,khte-the-box-96-5-fm,france-info,france-musique-1,france-bleu-provence,france-bleu-nord,france-bleu-alsace,france-bleu-haute-normandie,france-bleu-loire-ocean,france-bleu-armorique,france-bleu-breizh-izel,france-bleu-basse-normandie,france-bleu-isere,france-bleu-besancon,france-musique-la-jazz,fip-autour-du-jazz,fip-autour-du-rock,fip-autour-de-l-electro,radio-hoy,radiolab-costa-rica,radio-resonance,radio-rennes,radio-club,radio-valois-multien,fm-mirchi-dubai,big-106-2-fm,beat-97-8-fm,pulse-95-fm,tamil-89-4fm,fujairah-fm,radio-asia-94-7,radio-gilli-106-5fm,al-rabia-fm,al-arabiya-99-0,u-radio,radio-campus-47,radio-mne,radio-campus-pau,radio-campus-lorraine,radio-ballade-101-8,le-chantier,ouest-track,radio-active-100fm,radio-campus-orleans,radio-campus-grenoble-1,euradio,radio-balises,soar-radio,the-detroit-praise-network,wmnf,wmot,wmvy,wmwv,wncs,wncw,wnrn,wnxp,wocm,woxl-hd2,wqkl-fm,wral-f2,wrnr-fm,wrsi,wsge,wtts,wtyd-the-tide,wuin,wuky,wumb,wunc,wusm,wutc,wwct,wxpk,wyce,wyep,wyso,wzew,wzlo,wfuv,kink-1,kcmp,kutx,kxt,wers,kcsn,sun-radio,krvb,klrr-fm,wclz-fm,whrv,wdvx,kysl,wfpk,ksmf-fm,wevl,krcl,wcbe,ktao,waps,kdtr,ktbg,wbjb,kclc,wcnr,wkze,wdiy,khum,wlkr,kdnk,kvyn,wfhb,kkal,wfit-fm,wext,kaxe,kvnf,wjcu,kvna,knba,kjac,wklq-fm,wehm,wdst,kuwr,weru-fm-89-9-blue-hill,kpnd,krok,kbco,krfc,wbsd,krsh,kmms,kpnw,ksut,kywh-fm,morehead-state-public-radio-mspr,wmmm,wpya,wral-hd2,wtmd,the-coast-norfolk,kfmg,wclx-farm-fresh-radio,radio-tev,radio-superhits,kmyo-latino-mix-95-1-fm,radio-marilu,radio-capital,alif-alif-fm,trace-fm,nostalgie-fm,fajn-radio,syn-fm,niu-fm,radio-ellebore,radio-galere,radio-satelite,power-93-5-fm,mbc-fm-1,heart-fm-2,radio-easy-rock,90fm,hit-radio,radio-sawa,radio-malayalam-98-6fm,fm-107-urdu-radio-dil-se-desi,fm-107-urdu-radio,radio-olive-106-3-fm,radio-suno-91-7,radio-wigwam,br-schlager,kringvarp-foroya,voxpop,stream-fo-fm-98-7,cowboy-joe-radio-1,al-khaleejiya-1009,star-fm-2,radios-chretiennes-francophones-rcf,urbana-play-104-3-fm,edge-radio,radio-samobor,mix-fm-3,enjoy-33,exa-honduras,mid-west-radio,deep-dance-radio,openlab,starpoint-radio,birmingham-mountain-radio,kgsr-hd2-austin-city-limits-radio,kmtn-the-mountain-96-9-fm,wunc-music,wrsb-mega-97-5,3mdr,radio-party-groove,scanner-fm,xrp-radio,prospect-radio,wort-89-9-fm-community-radio,kser,kpoo,koop,knon,knmc,wxht-hot-102-7,whup,kmsu,khol,kdhx,kboo,wmsc,wmfo,trace-fm-1,athens-dee-jay,hit-88-9-fm,yes-91-2,loud-88-8,radio-abc,pure-fm-1,radio-2000,motsweding-fm,lesedi-fm,gagasi-fm,thobela-fm,punto-rojo-89-7-89-9-fm,klmo-98-9-fm,93-7-krq-tucson-krqq,unika-fm,gran-via-radio,one-dance,radioshakehit,one-fm-2,bucuresti-fm,ici-musique,kiss,dance-radio,radio-orbital,radio-nova-era,top-fm-1,wrock-96-3-cebu,rj-fm,martinique-1ere,1-fm-dance-one,radio-3i,radio-rottu-oberwallis,vibration-108,radio-neo1-1,radio-one-1,abc-classic,radio-adelaide-101-5fm,radio-austria,radio-mango,red-fm,east-coast-fm,shannonside-fm,max-fm-102-3,fg-chic,nrj-made-in-france,oui-fm-rock-inde,frequence-3,maxximum,radio-helsinki,fun-radio-1,radio-expres,europa-2,power-hit-radio-1,i-love-music,popradio-nrw,ufm-ultima-radio,beats-radio,global-urban-radio,beat-fm,umhlobo-wenene-fm,saules-radijas,rinse,radio-chilango,joe,melody-vintage-radio,smooth-chill,radio-zeta,magic-fm,virgin-radio-romania,digi-fm,rock-fm-3,radio-impuls,radyo-94-8-metropol-fm,zoo-radio-90-8,cosmoradio-95-1,best-92-6-fm,party-97-1-fm,radio-polis-99-4-fm,radio2funky,antenne-munster,del-sol-fm,radio-monte-carlo-930,del-plata-95-5-fm,fm-hit-90-3-fm,radio-disney-uruguay,wbai-fm,wvia,wruw-91-1-fm,wtju-91-1-fm,wrfl-88-1-fm,radio-concierto,93-7-fm-universo,cind-fm-indie-88,aktiv-radio,horizonte-107-9-fm,rfi-musique,unity-radio-92-8-fm,waev,dance-one,global-soul-radio,plus-radio-102-4,lulu-fm,gay-sa-radio,wqxr,ritmo-95-7,wlrn-91-3-fm,wpln-90-3-fm-nashville-public-radio,wsun-el-nuevo-zol-97-1,kasu-91-9-fm,duma-fm,atlantida-fm-florianopolis,m80,relax-fm,radio-silesia,world-radio-switzerland,nrj-electro,phare-fm,radio-brume,60s-gold,wowd,france-musique-baroque,ext-radio,wjmu-89-5-fm,berry-fm,graffiti-urban-radio,radio-activ-101-9-fm-1,atomic-radio,radio-g,sok-fm-104-8,c-rock-radio,ibiza-1-radio,codiac-fm-93-5,absolute-radio-country,radio-m,radio-mega-1,radio-campus-47-1,radar-radio-art-en-sort,plum-fm,net-radio,m-1-dance,european-hit-radio,radioor,kkdg-99-7-radio-free-durango",
         "startDate" : "2024-06-04",
          "endDate" : "2024-09-01",
        "offset" : 0,
        "limit" : 100}
dates = "0604_0901"


input_file = "Input/list_uuidNovember24.csv"
df = pd.read_csv(input_file)
base_url = "https://customer.api.soundcharts.com"


column_names = ['obs_no','uuid','airedAt','duration','radio_slug','radio_name','radio_cityName','radio_countryCode','radio_countryName', 'radio_timeZone']
file = open(f'Output/radio_{dates}.csv', 'a', newline='', encoding="UTF-8")
writer = csv.writer(file)
writer.writerow(column_names)

row_written = False
tot_items = 0
uuid_count = 0
start = 0
end = 10

for uuid in df['uuid'][start:end]:
    try:
        uuid_count =  uuid_count + 1
        if uuid_count%20==0:
            print(f"url hits = {uuid_count}")
            print(tot_items)
        #print("On uuid ", uuid_count, " of ", len(id_list))
        # Set initial url
        url = f"https://customer.api.soundcharts.com/api/v2/song/{uuid}/broadcasts"
    
        r = 1
        u_count = 0
        while url:
            if r == 1:
                response = requests.get(url,params= params,headers=headers,auth = (username,password))
            else:
                response = requests.get(url,headers=headers,auth = (username,password))
            u_count += 1
    
            if response.status_code == 404:
                url = None
                break
                
            if response.status_code == 200:
                data = response.json()
                uuid = data['related']['uuid']
                nextlink = data['page']['next']
                tot_items = tot_items + len(data['items'])
                for row in data['items']:
                    one_row = [r, uuid, row['airedAt'], row['duration'], row['radio']['slug'], row['radio']['name'], row['radio']['cityName'], row['radio']['countryCode'], row['radio']['countryName'], row['radio']['timeZone']]
                    writer.writerow(one_row)
                    row_written =  True
            
                    if row_written is not True:
                        print("Row not written:", r, "for uuid: ", uuid)
                    #print(r)
                    r = r + 1
    
                if nextlink:
                    url = f'{base_url}{nextlink}'
                    #u_count = u_count + 1
                else:
                    url = None
                #print(url)
    
                #if u_count > 2:
                    #print("count is greater than 2")
                    #break
                #print(tot_items)
    except:
        f = open("radioslugerrors","a")
        f.write(f"{uuid} \n")
        f.close()
        

file.close()
