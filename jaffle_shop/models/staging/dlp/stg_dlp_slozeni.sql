with slozeni as (

    select * from {{ ref('base_dlp_slozeni') }}

),

latky as (

    select kod_latky, nazev as latka_nazev, nazev_inn as latka_nazev_inn from {{ ref('base_dlp_latky') }}

),

jednotky as (

    select un, nazev as jednotka_nazev from {{ ref('base_dlp_jednotky') }}

),

slozenipriznak as (

    select s, vyznam as priznak_vyznam from {{ ref('base_dlp_slozenipriznak') }}

)

select
    slozeni.kod_sukl,
    slozeni.kod_latky,
    latky.latka_nazev,
    latky.latka_nazev_inn,
    slozeni.sq,
    slozeni.s,
    slozenipriznak.priznak_vyznam,
    slozeni.amnt_od,
    slozeni.amnt,
    slozeni.un,
    jednotky.jednotka_nazev

from slozeni
left join latky on slozeni.kod_latky = latky.kod_latky
left join jednotky on slozeni.un = jednotky.un
left join slozenipriznak on slozeni.s = slozenipriznak.s
