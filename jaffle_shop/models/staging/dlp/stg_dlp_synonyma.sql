with synonyma as (

    select * from {{ ref('base_dlp_synonyma') }}

),

latky as (

    select kod_latky, nazev as latka_nazev from {{ ref('base_dlp_latky') }}

)

select
    synonyma.kod_latky,
    latky.latka_nazev,
    synonyma.sq,
    synonyma.zdroj,
    synonyma.nazev as synonymum

from synonyma
left join latky on synonyma.kod_latky = latky.kod_latky
