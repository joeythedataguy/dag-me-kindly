with source as (

    select * from {{ source('dlp', 'dim_dlp_zruseneregistrace') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        nazev,
        cesta,
        forma,
        sila,
        registracni_cislo,
        soubezny_dovoz,
        mrp_cislo,
        typ_registrace,
        pravni_zaklad_registrace,
        drzitel,
        zeme_drzitele,
        try_strptime(konec_platnosti_registrace, '%d.%m.%Y')::date as konec_platnosti_registrace,
        stav_registrace

    from source

)

select * from renamed
