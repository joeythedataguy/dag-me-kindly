with lek13 as (

    select * from {{ ref('base_lek13') }}

)

select
    obdobi,
    typ_hlaseni,
    atc7,
    kod_sukl,
    nazev_pripravku,
    doplnek_nazvu,
    drzitel_registrace,
    zeme,
    pocet_baleni,
    nakupni_cena_bez_dph,
    konecna_prodejni_cena_s_dph,
    pocet_ddd_baleni,
    zpusob_vydeje,
    is_hrazeno

from lek13
