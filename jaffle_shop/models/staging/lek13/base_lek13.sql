with source as (

    select * from {{ source('lek13', 'src_lek13') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_strptime(obdobi || '.01', '%Y.%m.%d')::date as obdobi,
        typ_hlaseni,
        atc7,
        kod_sukl,
        nazev_pripravku,
        doplnek_nazvu,
        drzitel_registrace,
        zeme,
        try_cast(pocet_baleni as integer) as pocet_baleni,
        try_cast(replace(nakupni_cena_bez_dph, ',', '.') as decimal(12, 2)) as nakupni_cena_bez_dph,
        try_cast(replace(konecna_prodejni_cena_s_dph, ',', '.') as decimal(12, 2)) as konecna_prodejni_cena_s_dph,
        try_cast(replace(pocet_ddd_baleni, ',', '.') as decimal(12, 4)) as pocet_ddd_baleni,
        zpusob_vydeje,
        hrazeno = 'Ano' as is_hrazeno

    from source

)

select * from renamed
