with zruseneregistrace as (

    select * from {{ ref('base_dlp_zruseneregistrace') }}

),

zeme as (

    select zem, nazev as zeme_drzitele_nazev from {{ ref('base_dlp_zeme') }}

),

pravnizakladreg as (

    select kod, nazev as pravni_zaklad_registrace_nazev from {{ ref('base_dlp_pravnizakladreg') }}

),

stavyreg as (

    select reg, nazev as stav_registrace_nazev from {{ ref('base_dlp_stavyreg') }}

),

drzitel as (

    select zkr_org, zem, nazev as drzitel_nazev from {{ ref('base_dlp_organizace') }}

)

select
    zruseneregistrace.nazev,
    zruseneregistrace.cesta,
    zruseneregistrace.forma,
    zruseneregistrace.sila,
    zruseneregistrace.registracni_cislo,
    zruseneregistrace.soubezny_dovoz,
    zruseneregistrace.mrp_cislo,
    zruseneregistrace.typ_registrace,
    zruseneregistrace.pravni_zaklad_registrace,
    pravnizakladreg.pravni_zaklad_registrace_nazev,
    zruseneregistrace.drzitel,
    zruseneregistrace.zeme_drzitele,
    zeme.zeme_drzitele_nazev,
    drzitel.drzitel_nazev,
    zruseneregistrace.konec_platnosti_registrace,
    zruseneregistrace.stav_registrace,
    stavyreg.stav_registrace_nazev

from zruseneregistrace
left join zeme on zruseneregistrace.zeme_drzitele = zeme.zem
left join pravnizakladreg on zruseneregistrace.pravni_zaklad_registrace = pravnizakladreg.kod
left join stavyreg on zruseneregistrace.stav_registrace = stavyreg.reg
left join drzitel on zruseneregistrace.drzitel = drzitel.zkr_org and zruseneregistrace.zeme_drzitele = drzitel.zem
