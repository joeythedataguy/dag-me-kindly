with soli as (

    select * from {{ ref('base_dlp_soli') }}

),

zakladni_latka as (

    select kod_latky, nazev as zakladni_latka_nazev from {{ ref('base_dlp_latky') }}

),

sul as (

    select kod_latky, nazev as sul_nazev from {{ ref('base_dlp_latky') }}

)

select
    soli.kod_latky,
    zakladni_latka.zakladni_latka_nazev,
    soli.kod_soli,
    sul.sul_nazev

from soli
left join zakladni_latka on soli.kod_latky = zakladni_latka.kod_latky
left join sul on soli.kod_soli = sul.kod_latky
