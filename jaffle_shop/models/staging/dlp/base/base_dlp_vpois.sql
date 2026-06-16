with source as (

    select * from {{ source('dlp', 'dim_dlp_vpois') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        kod_sukl,
        vpois_nazev_spolecnosti,
        vpois_www,
        vpois_email,
        vpois_tel

    from source

)

select * from renamed
