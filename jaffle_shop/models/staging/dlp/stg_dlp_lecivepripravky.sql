with lecivepripravky as (

    select * from {{ ref('base_dlp_lecivepripravky') }}

),

cesty as (

    select cesta, nazev as cesta_nazev from {{ ref('base_dlp_cesty') }}

),

formy as (

    select forma, nazev as forma_nazev from {{ ref('base_dlp_formy') }}

),

obaly as (

    select obal, nazev as obal_nazev from {{ ref('base_dlp_obaly') }}

),

atc as (

    select atc, nazev as atc_who_nazev from {{ ref('base_dlp_atc') }}

),

typlp as (

    select typ_lp, nazev as typ_lp_nazev from {{ ref('base_dlp_typlp') }}

),

vydej as (

    select vydej, nazev as vydej_nazev from {{ ref('base_dlp_vydej') }}

),

stavyreg as (

    select reg, nazev as reg_nazev from {{ ref('base_dlp_stavyreg') }}

),

zavislost as (

    select zav, nazev as zav_nazev from {{ ref('base_dlp_zavislost') }}

),

doping as (

    select doping, nazev as doping_nazev from {{ ref('base_dlp_doping') }}

),

narvla as (

    select narvla, nazev as narvla_nazev from {{ ref('base_dlp_narvla') }}

),

pravnizakladreg as (

    select kod, nazev as pravni_zaklad_registrace_nazev from {{ ref('base_dlp_pravnizakladreg') }}

),

indikacniskupiny as (

    select indsk, nazev as indikacni_skupina_nazev from {{ ref('base_dlp_indikacniskupiny') }}

),

regproc as (

    select reg_proc, nazev as reg_proc_nazev from {{ ref('base_dlp_regproc') }}

),

zeme_registrace as (

    select zem, nazev as zemdrz_nazev from {{ ref('base_dlp_zeme') }}

),

zeme_aktualni as (

    select zem, nazev as akt_zem_nazev from {{ ref('base_dlp_zeme') }}

),

drzitel as (

    select zkr_org, zem, nazev as drzitel_nazev from {{ ref('base_dlp_organizace') }}

)

select
    lecivepripravky.kod_sukl,
    lecivepripravky.h,
    lecivepripravky.nazev,
    lecivepripravky.sila,
    lecivepripravky.forma,
    formy.forma_nazev,
    lecivepripravky.baleni,
    lecivepripravky.cesta,
    cesty.cesta_nazev,
    lecivepripravky.doplnek,
    lecivepripravky.obal,
    obaly.obal_nazev,
    lecivepripravky.drz,
    lecivepripravky.zemdrz,
    zeme_registrace.zemdrz_nazev,
    drzitel.drzitel_nazev,
    lecivepripravky.akt_drz,
    lecivepripravky.akt_zem,
    zeme_aktualni.akt_zem_nazev,
    lecivepripravky.reg,
    stavyreg.reg_nazev,
    lecivepripravky.v_platdo,
    lecivepripravky.neomez,
    lecivepripravky.uvadenido,
    lecivepripravky.indikacni_skupina,
    indikacniskupiny.indikacni_skupina_nazev,
    lecivepripravky.atc_who,
    atc.atc_who_nazev,
    lecivepripravky.rc,
    lecivepripravky.sdov,
    lecivepripravky.sdov_dod,
    lecivepripravky.sdov_zem,
    lecivepripravky.reg_proc,
    regproc.reg_proc_nazev,
    lecivepripravky.dddamnt_who,
    lecivepripravky.dddun_who,
    lecivepripravky.dddp_who,
    lecivepripravky.zdroj_who,
    lecivepripravky.ll,
    lecivepripravky.vydej,
    vydej.vydej_nazev,
    lecivepripravky.zav,
    zavislost.zav_nazev,
    lecivepripravky.doping,
    doping.doping_nazev,
    lecivepripravky.narvla,
    narvla.narvla_nazev,
    lecivepripravky.dodavky,
    lecivepripravky.ean,
    lecivepripravky.braillovo_pismo,
    lecivepripravky.exp,
    lecivepripravky.exp_t,
    lecivepripravky.nazev_reg,
    lecivepripravky.mrp_cislo,
    lecivepripravky.pravni_zaklad_registrace,
    pravnizakladreg.pravni_zaklad_registrace_nazev,
    lecivepripravky.ochranny_prvek,
    lecivepripravky.omezeni_preskripce_smp,
    lecivepripravky.typ_lp,
    typlp.typ_lp_nazev

from lecivepripravky
left join cesty on lecivepripravky.cesta = cesty.cesta
left join formy on lecivepripravky.forma = formy.forma
left join obaly on lecivepripravky.obal = obaly.obal
left join atc on lecivepripravky.atc_who = atc.atc
left join typlp on lecivepripravky.typ_lp = typlp.typ_lp
left join vydej on lecivepripravky.vydej = vydej.vydej
left join stavyreg on lecivepripravky.reg = stavyreg.reg
left join zavislost on lecivepripravky.zav = zavislost.zav
left join doping on lecivepripravky.doping = doping.doping
left join narvla on lecivepripravky.narvla = narvla.narvla
left join pravnizakladreg on lecivepripravky.pravni_zaklad_registrace = pravnizakladreg.kod
left join indikacniskupiny on lecivepripravky.indikacni_skupina = indikacniskupiny.indsk
left join regproc on lecivepripravky.reg_proc = regproc.reg_proc
left join zeme_registrace on lecivepripravky.zemdrz = zeme_registrace.zem
left join zeme_aktualni on lecivepripravky.akt_zem = zeme_aktualni.zem
left join drzitel on lecivepripravky.drz = drzitel.zkr_org and lecivepripravky.zemdrz = drzitel.zem
