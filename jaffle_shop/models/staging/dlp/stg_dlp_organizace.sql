with organizace as (

    select * from {{ ref('base_dlp_organizace') }}

),

zeme as (

    select zem, nazev as zeme_nazev from {{ ref('base_dlp_zeme') }}

)

select
    organizace.zkr_org,
    organizace.zem,
    zeme.zeme_nazev,
    organizace.nazev,
    organizace.is_vyrobce,
    organizace.is_drzitel

from organizace
left join zeme on organizace.zem = zeme.zem
