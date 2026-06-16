with source as (

    select * from {{ source('dlp', 'dim_dlp_synonyma') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        try_cast(kod_latky as integer) as kod_latky,
        try_cast(sq as integer) as sq,
        zdroj,
        nazev

    from source

)

select * from renamed
