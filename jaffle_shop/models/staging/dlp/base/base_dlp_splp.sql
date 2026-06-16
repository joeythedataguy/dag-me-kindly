with source as (

    select * from {{ source('dlp', 'dim_dlp_splp') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        kod_sukl,
        try_strptime(datod, '%d.%m.%Y')::date as datod,
        try_strptime(datdo, '%d.%m.%Y')::date as datdo,
        povol_baleni,
        ucel,
        pracoviste,
        distributor,
        poznamka,
        predkladatel,
        vyrobce

    from source

)

select * from renamed
