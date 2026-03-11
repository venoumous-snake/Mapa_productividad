library(dplyr)
library(leaflet)
library(htmlwidgets)
library(openxlsx)
library(htmltools)
library(stringr)
library(stringi)
library(DBI)
library(duckdb)
library(arrow)
library(tidyr)
library(tibble)

# =========================
# 0) COLORES INSTITUCIONALES
# =========================
COL_VINO   <- "#7A1F2B"
COL_DORADO <- "#C9A227"
COL_BLANCO <- "#FFFFFF"

# =========================
# 0.1) URL del módulo de documentos (Proyecto 72)
# =========================
DOCS_URL <- "https://paolagonzalez.shinyapps.io/72_App_productividad/"

# =========================
# 0.2) FECHAS (RANGO POR AÑO)
# =========================
FECHA_FIN_2024 <- as.Date("2024-03-05")
FECHA_FIN_2025 <- as.Date("2025-03-05")
FECHA_FIN_2026 <- as.Date("2026-03-05")

fecha_corte_lbl <- paste0("al ", format(FECHA_FIN_2026, "%d de %B"))
anio_corte <- format(FECHA_FIN_2026, "%Y")

# =========================
# 0.3) RUTAS (PRODUCTIVIDAD + BRECHA + INFRA)
# =========================
BRECHA_PARQUET <- "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Comando Florence Nightingale/Proyectos/5_Censo/Anterior/Analisis Brecha/Data/Brechas/Brecha nacional/brecha_aritmetica_min.parquet"
INFRA_XLSX <- "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Repositorio de Datos/Infraestructura/infraestructura.xlsx"

# 2024
PQ_2024  <- "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Repositorio de Datos/Productividad/Latencia cirugias/latencia_procedimientos_quirurgicos_2024.parquet"
GEN_2024 <- "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Repositorio de Datos/Productividad/Latencia consultas/latencia_consultas_2024/tipo_consulta=general/part-0.parquet"
ESP_2024 <- "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Repositorio de Datos/Productividad/Latencia consultas/latencia_consultas_2024/tipo_consulta=especialidad/part-0.parquet"

# 2025
PQ_2025  <- "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Repositorio de Datos/Productividad/Latencia cirugias/latencia_procedimientos_quirurgicos_2025.parquet"
LAT_2025 <- "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Repositorio de Datos/Productividad/Latencia consultas/latencia_consultas_2025.parquet"

# 2026
PQ_2026  <- "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Repositorio de Datos/Productividad/Latencia cirugias/latencia_procedimientos_quirurgicos_2026.parquet"
LAT_2026 <- "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Repositorio de Datos/Productividad/Latencia consultas/latencia_consultas_2026.parquet"

# =========================
# Helpers
# =========================
sql_path <- function(p) gsub("\\\\", "/", normalizePath(p, winslash = "/", mustWork = FALSE))

fmt_int <- function(x){
  x <- suppressWarnings(as.numeric(x))
  ifelse(is.na(x), 0, x)
}

fmt_num_html <- function(x){
  x <- fmt_int(x)
  formatC(x, format = "f", digits = 0, big.mark = ",")
}

fmt_pct_html <- function(num, den, digits = 1){
  num <- suppressWarnings(as.numeric(num))
  den <- suppressWarnings(as.numeric(den))
  bad <- is.na(num) | is.na(den) | den <= 0
  ifelse(bad, "sin información",
         paste0(formatC(100 * num / den, format = "f", digits = digits), "%"))
}

normaliza_clues <- function(x) {
  x <- as.character(x)
  x <- toupper(trimws(x))
  x[is.na(x)] <- ""
  x
}

# =========================
# lógica aplica/no aplica por nivel
# =========================
nivel_num_from_txt <- function(x){
  x <- tolower(trimws(as.character(x)))
  x <- stringi::stri_trans_general(x, "Latin-ASCII")
  out <- ifelse(grepl("^1|primer", x), 1L,
                ifelse(grepl("^2|segundo", x), 2L,
                       ifelse(grepl("^3|tercer", x), 3L, NA_integer_)))
  out
}

fmt_num_commas <- function(x){
  x <- suppressWarnings(as.numeric(x))
  formatC(x, format = "f", digits = 0, big.mark = ",")
}

fmt_val_aplica <- function(x, aplica){
  x <- suppressWarnings(as.numeric(x))
  ifelse(!aplica, "-",
         ifelse(is.na(x) | x == 0, "sin información", fmt_num_commas(x)))
}

fmt_pct_aplica <- function(num, den, aplica, digits = 1){
  num <- suppressWarnings(as.numeric(num))
  den <- suppressWarnings(as.numeric(den))
  ifelse(!aplica, "-",
         ifelse(is.na(num) | num == 0 | is.na(den) | den <= 0,
                "sin información",
                paste0(formatC(100 * num / den, format = "f", digits = digits), "%")))
}

# =========================
# 1) CARGA DE DATOS (CLUES)
# =========================
clues <- read.xlsx(
  "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Repositorio de Datos/CLUES/clues.xlsx"
)

# =========================
# 2) BRECHA -> RESUMEN
# =========================
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
DBI::dbExecute(con, "PRAGMA threads=6;")
DBI::dbExecute(con, "PRAGMA enable_object_cache=true;")

on.exit({
  try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
}, add = TRUE)

brecha_path <- sql_path(BRECHA_PARQUET)

brecha_sum <- DBI::dbGetQuery(con, sprintf("
  SELECT
    upper(trim(clues_imb)) AS clues_imb,
    CASE
      WHEN upper(trim(codigo_cnpm)) LIKE 'CG%%' THEN 'CG'
      WHEN upper(trim(codigo_cnpm)) LIKE 'MG%%' THEN 'MG'
      WHEN upper(trim(codigo_cnpm)) LIKE 'ME%%' THEN 'ME'
      WHEN upper(trim(codigo_cnpm)) LIKE 'EN%%' THEN 'EN'
      ELSE 'Otros'
    END AS grupo,
    SUM(COALESCE(try_cast(total_real  AS DOUBLE), 0)) AS ocupado,
    SUM(COALESCE(try_cast(total_ideal AS DOUBLE), 0)) AS ideal,
    SUM(COALESCE(try_cast(brecha     AS DOUBLE), 0)) AS brecha,
    SUM(COALESCE(try_cast(excedente  AS DOUBLE), 0)) AS excedente
  FROM read_parquet('%s')
  GROUP BY 1,2
", brecha_path)) %>%
  tibble::as_tibble()

brecha_tablas <- brecha_sum %>%
  group_by(clues_imb) %>%
  summarise(
    tabla_brecha = {
      o <- cur_data_all() %>%
        mutate(
          ocupado    = ifelse(is.na(ocupado) | ocupado == 0, "sin información", fmt_num_html(ocupado)),
          ideal      = ifelse(is.na(ideal)   | ideal   == 0, "sin información", fmt_num_html(ideal)),
          brecha     = ifelse(is.na(brecha)  | brecha  == 0, "sin información", fmt_num_html(brecha)),
          excedente  = ifelse(is.na(excedente)|excedente==0, "sin información", fmt_num_html(excedente))
        ) %>%
        select(grupo, ocupado, ideal, brecha, excedente)
      
      o$grupo <- factor(o$grupo, levels = c("CG","MG","ME","EN","Otros"))
      o <- o %>% arrange(grupo)
      
      paste0(
        "<div style='font-weight:900; margin-top:10px; margin-bottom:6px;'>Brecha</div>",
        "<table style='width:100%; border-collapse:collapse; font-size:10.5px;'>",
        "<thead><tr>",
        "<th style='text-align:left; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Grupo</th>",
        "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Ocupado</th>",
        "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Ideal</th>",
        "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Brecha</th>",
        "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Excedente</th>",
        "</tr></thead><tbody>",
        paste0(
          "<tr>",
          "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>", o$grupo, "</td>",
          "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", o$ocupado, "</td>",
          "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", o$ideal, "</td>",
          "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", o$brecha, "</td>",
          "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", o$excedente, "</td>",
          "</tr>",
          collapse = ""
        ),
        "</tbody></table>"
      )
    },
    .groups = "drop"
  )

# =========================
# 2.1) INFRAESTRUCTURA
# =========================
infra <- read.xlsx(INFRA_XLSX) %>% tibble::as_tibble()

col_clues_infra <- dplyr::case_when(
  "clues_imb" %in% names(infra) ~ "clues_imb",
  "CLUES_IMB" %in% names(infra) ~ "CLUES_IMB",
  "clues"     %in% names(infra) ~ "clues",
  "CLUES"     %in% names(infra) ~ "CLUES",
  TRUE ~ NA_character_
)

if (is.na(col_clues_infra)) {
  stop("No encontré columna de CLUES en infraestructura.xlsx. Columnas:\n- ",
       paste(names(infra), collapse = "\n- "))
}

infra <- infra %>%
  mutate(clues_imb = normaliza_clues(.data[[col_clues_infra]]))

cols_infra_need <- c(
  "consultorios_generales_habilitados",
  "consultorios_generales_inhabilitados",
  "consultorios_de_especialidad_habilitados",
  "consultorios_de_especialidad_inhabilitados",
  "quirofanos_habilitados",
  "quirofanos_inhabilitados",
  "salas_tococirugia_habilitadas",
  "salas_tococirugia_inhabilitadas",
  "salas_expulsion_habilitadas",
  "salas_expulsion_inhabilitadas",
  "camas_censables_habilitadas",
  "camas_censables_inhabilitadas"
)

infra_min <- infra %>%
  select(clues_imb, any_of(cols_infra_need)) %>%
  distinct(clues_imb, .keep_all = TRUE) %>%
  mutate(across(-clues_imb, fmt_int))

infra_tablas <- infra_min %>%
  transmute(
    clues_imb,
    tabla_infra = paste0(
      "<div style='font-weight:900; margin-top:10px; margin-bottom:6px;'>Infraestructura</div>",
      "<table style='width:100%; border-collapse:collapse; font-size:10.5px;'>",
      "<thead><tr>",
      "<th style='text-align:left; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Recurso</th>",
      "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Hab.</th>",
      "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Inhab.</th>",
      "</tr></thead><tbody>",
      
      "<tr>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>Consultorios generales</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(consultorios_generales_habilitados), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(consultorios_generales_inhabilitados), "</td>",
      "</tr>",
      
      "<tr>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>Consultorios especialidad</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(consultorios_de_especialidad_habilitados), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(consultorios_de_especialidad_inhabilitados), "</td>",
      "</tr>",
      
      "<tr>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>Quirófanos</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(quirofanos_habilitados), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(quirofanos_inhabilitados), "</td>",
      "</tr>",
      
      "<tr>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>Salas tococirugía</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(salas_tococirugia_habilitadas), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(salas_tococirugia_inhabilitadas), "</td>",
      "</tr>",
      
      "<tr>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>Salas expulsión</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(salas_expulsion_habilitadas), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(salas_expulsion_inhabilitadas), "</td>",
      "</tr>",
      
      "<tr>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>Camas censables</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(camas_censables_habilitadas), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_num_html(camas_censables_inhabilitadas), "</td>",
      "</tr>",
      
      "</tbody></table>"
    )
  )

# =========================
# 2.2) METAS
# =========================
metas <- arrow::read_parquet(
  "C:/Users/josue.morales/IMSS-BIENESTAR/División de Procesamiento de información - Comando Florence Nightingale/Proyectos/73 Mapa Clues/HTML mapa/Mapa_Clues/metas_2026.parquet"
) %>%
  tibble::as_tibble()

metas_min <- metas %>%
  transmute(
    clues_imb = normaliza_clues(clues),
    meta_general_anual      = fmt_int(meta_general_anual),
    meta_especialidad_anual = fmt_int(meta_especialidad_anual),
    meta_cirugia_anual      = fmt_int(meta_cirugia_anual),
    meta_egresos_anual      = fmt_int(meta_egresos_anual)
  ) %>%
  group_by(clues_imb) %>%
  summarise(
    meta_general_anual      = max(meta_general_anual,      na.rm = TRUE),
    meta_especialidad_anual = max(meta_especialidad_anual, na.rm = TRUE),
    meta_cirugia_anual      = max(meta_cirugia_anual,      na.rm = TRUE),
    meta_egresos_anual      = max(meta_egresos_anual,      na.rm = TRUE),
    .groups = "drop"
  )

metas_por_clues <- metas_min %>%
  group_by(clues_imb) %>%
  summarise(meta_tbl = list(cur_data_all()), .groups = "drop")

# =========================
# 3) PRODUCTIVIDAD
# =========================
pq_sel <- function(path_pq){
  cols <- DBI::dbGetQuery(con, sprintf("DESCRIBE SELECT * FROM read_parquet('%s') LIMIT 1", sql_path(path_pq)))
  if ("conteo_diario" %in% tolower(cols$column_name)) {
    "SUM(COALESCE(try_cast(conteo_diario AS DOUBLE),0))"
  } else {
    "COUNT(*)"
  }
}

prod_2024 <- DBI::dbGetQuery(con, sprintf("
  WITH
  gen AS (
    SELECT upper(trim(clues)) AS clues_imb, COUNT(*) AS general
    FROM read_parquet('%s')
    WHERE CAST(fecha_consulta AS DATE) >= DATE '2024-01-01'
      AND CAST(fecha_consulta AS DATE) <= DATE '%s'
    GROUP BY 1
  ),
  esp AS (
    SELECT upper(trim(clues)) AS clues_imb, COUNT(*) AS especialidad
    FROM read_parquet('%s')
    WHERE CAST(fecha_consulta AS DATE) >= DATE '2024-01-01'
      AND CAST(fecha_consulta AS DATE) <= DATE '%s'
    GROUP BY 1
  ),
  qx AS (
    SELECT upper(trim(clues)) AS clues_imb, %s AS qx
    FROM read_parquet('%s')
    WHERE CAST(fecha_egreso AS DATE) >= DATE '2024-01-01'
      AND CAST(fecha_egreso AS DATE) <= DATE '%s'
    GROUP BY 1
  ),
  base AS (
    SELECT clues_imb FROM gen
    UNION SELECT clues_imb FROM esp
    UNION SELECT clues_imb FROM qx
  )
  SELECT
    b.clues_imb,
    2024 AS anio,
    COALESCE(g.general,0) AS general,
    COALESCE(e.especialidad,0) AS especialidad,
    COALESCE(q.qx,0) AS qx,
    COALESCE(g.general,0)+COALESCE(e.especialidad,0) AS total
  FROM base b
  LEFT JOIN gen g USING(clues_imb)
  LEFT JOIN esp e USING(clues_imb)
  LEFT JOIN qx  q USING(clues_imb)
", sql_path(GEN_2024), format(FECHA_FIN_2024, "%Y-%m-%d"),
                                          sql_path(ESP_2024), format(FECHA_FIN_2024, "%Y-%m-%d"),
                                          pq_sel(PQ_2024), sql_path(PQ_2024), format(FECHA_FIN_2024, "%Y-%m-%d")
)) %>% tibble::as_tibble()

prod_25_26 <- function(lat_path, pq_path, anio, fecha_fin){
  DBI::dbGetQuery(con, sprintf("
    WITH
    lat AS (
      SELECT upper(trim(clues)) AS clues_imb,
             lower(CAST(tipo_consulta AS VARCHAR)) AS tipo,
             CAST(fecha_consulta AS DATE) AS f
      FROM read_parquet('%s')
      WHERE CAST(fecha_consulta AS DATE) >= DATE '%d-01-01'
        AND CAST(fecha_consulta AS DATE) <= DATE '%s'
    ),
    gen AS (
      SELECT clues_imb, COUNT(*) AS general
      FROM lat WHERE tipo='general'
      GROUP BY 1
    ),
    esp AS (
      SELECT clues_imb, COUNT(*) AS especialidad
      FROM lat WHERE tipo='especialidad'
      GROUP BY 1
    ),
    qx AS (
      SELECT upper(trim(clues)) AS clues_imb, %s AS qx
      FROM read_parquet('%s')
      WHERE CAST(fecha_egreso AS DATE) >= DATE '%d-01-01'
        AND CAST(fecha_egreso AS DATE) <= DATE '%s'
      GROUP BY 1
    ),
    base AS (
      SELECT clues_imb FROM gen
      UNION SELECT clues_imb FROM esp
      UNION SELECT clues_imb FROM qx
    )
    SELECT
      b.clues_imb,
      %d AS anio,
      COALESCE(g.general,0) AS general,
      COALESCE(e.especialidad,0) AS especialidad,
      COALESCE(q.qx,0) AS qx,
      COALESCE(g.general,0)+COALESCE(e.especialidad,0) AS total
    FROM base b
    LEFT JOIN gen g USING(clues_imb)
    LEFT JOIN esp e USING(clues_imb)
    LEFT JOIN qx  q USING(clues_imb)
  ", sql_path(lat_path), as.integer(anio), format(fecha_fin, "%Y-%m-%d"),
                               pq_sel(pq_path), sql_path(pq_path), as.integer(anio), format(fecha_fin, "%Y-%m-%d"),
                               as.integer(anio)
  )) %>% tibble::as_tibble()
}

prod_2025 <- prod_25_26(LAT_2025, PQ_2025, 2025, FECHA_FIN_2025)
prod_2026 <- prod_25_26(LAT_2026, PQ_2026, 2026, FECHA_FIN_2026)

prod_all <- bind_rows(prod_2024, prod_2025, prod_2026) %>%
  mutate(clues_imb = toupper(trimws(as.character(clues_imb))))

prod_wide <- prod_all %>%
  mutate(
    general      = fmt_int(general),
    especialidad = fmt_int(especialidad),
    qx           = fmt_int(qx),
    total        = fmt_int(total)
  ) %>%
  select(clues_imb, anio, total, general, especialidad, qx) %>%
  tidyr::pivot_wider(
    names_from = anio,
    values_from = c(total, general, especialidad, qx),
    values_fill = 0
  )

# =========================
# 4) BASE PARA MAPA
# =========================
macro_map <- tibble::tibble(
  macro_key   = c("hospitales", "clinicas", "unidades_moviles", "otros"),
  macro_label = c("Hospitales", "Clínicas", "Unidades Móviles", "Otros"),
  macro_color = c("#006657", "#7A1F2B", "#DEAE36", "#6B7280")
)
macro_keys <- macro_map$macro_key

darken_hex <- function(hex, f = 0.78){
  hex <- gsub("#","",hex)
  r <- strtoi(substr(hex,1,2),16L); g <- strtoi(substr(hex,3,4),16L); b <- strtoi(substr(hex,5,6),16L)
  r <- pmax(0, floor(r*f)); g <- pmax(0, floor(g*f)); b <- pmax(0, floor(b*f))
  sprintf("#%02X%02X%02X", r,g,b)
}
macro_map <- macro_map %>% mutate(macro_border = vapply(macro_color, darken_hex, character(1)))

pane_for <- function(macro_key, halo = FALSE){
  key <- as.character(macro_key)
  paste0("pane_", key, if (halo) "_halo" else "")
}

clues_mapa <- clues %>%
  mutate(
    estatus_simple = ifelse(estatus_de_operacion == "EN OPERACION", "EN OPERACION", "FUERA DE OPERACION"),
    latitud   = suppressWarnings(as.numeric(latitud)),
    longitud  = suppressWarnings(as.numeric(longitud)),
    clues_imb = toupper(trimws(as.character(clues_imb))),
    search_label = paste0(clues_imb, " | ", nombre_de_la_unidad, " | ", entidad),
    
    categoria_clean = str_to_title(trimws(as.character(categoria_gerencial))),
    categoria_clean = ifelse(is.na(categoria_clean) | categoria_clean == "", "Na", categoria_clean),
    
    categoria_key = categoria_clean %>%
      str_replace_all("[’´`]", "'") %>%
      str_to_lower() %>%
      stringi::stri_trans_general("Latin-ASCII") %>%
      trimws(),
    
    categoria_macro_key = case_when(
      categoria_key %in% c("general", "generales") ~ "hospitales",
      categoria_key %in% c("hraes", "materno infantil", "pediatrico", "basico comunitario") ~ "hospitales",
      categoria_key %in% c("nucleos", "servicios ampliados oncologia", "servicios ampliados oncologicos") ~ "clinicas",
      categoria_key %in% c("unidades moviles", "unidad movil") ~ "unidades_moviles",
      categoria_key %in% c("uneme", "unemes", "psiquiatrico", "psiquiatricos", "servicios ampliados", "na") ~ "otros",
      TRUE ~ "otros"
    ),
    
    nivel_num = nivel_num_from_txt(nivel_atencion),
    nivel_num = ifelse(is.na(nivel_num), 3L, nivel_num),
    
    pane_main = pane_for(categoria_macro_key, halo = FALSE),
    pane_halo = pane_for(categoria_macro_key, halo = TRUE),
    size_mult = ifelse(categoria_macro_key == "hospitales", 1.30, 1.00)
  ) %>%
  filter(!is.na(latitud), !is.na(longitud)) %>%
  left_join(macro_map,     by = c("categoria_macro_key" = "macro_key")) %>%
  left_join(brecha_tablas, by = "clues_imb") %>%
  left_join(prod_wide,     by = "clues_imb") %>%
  left_join(infra_tablas,  by = "clues_imb") %>%
  left_join(metas_por_clues, by = "clues_imb") %>%
  mutate(
    macro_label  = ifelse(is.na(macro_label), "Otros", macro_label),
    macro_color  = ifelse(is.na(macro_color), "#6B7280", macro_color),
    macro_border = ifelse(is.na(macro_border), darken_hex("#6B7280"), macro_border),
    macro_label  = factor(macro_label, levels = macro_map$macro_label),
    
    total_2026 = fmt_int(total_2026),
    total_2025 = fmt_int(total_2025),
    total_2024 = fmt_int(total_2024),
    
    general_2026 = fmt_int(general_2026),
    general_2025 = fmt_int(general_2025),
    general_2024 = fmt_int(general_2024),
    
    especialidad_2026 = fmt_int(especialidad_2026),
    especialidad_2025 = fmt_int(especialidad_2025),
    especialidad_2024 = fmt_int(especialidad_2024),
    
    qx_2026 = fmt_int(qx_2026),
    qx_2025 = fmt_int(qx_2025),
    qx_2024 = fmt_int(qx_2024),
    
    tabla_brecha = ifelse(is.na(tabla_brecha), "", tabla_brecha),
    tabla_infra  = ifelse(is.na(tabla_infra),  "", tabla_infra)
  )

# =========================
# 5) POPUP HTML
# =========================
clues_mapa <- clues_mapa %>%
  mutate(
    color_estatus = ifelse(estatus_simple == "EN OPERACION", "#16A34A", "#B91C1C"),
    ap_g = TRUE,
    ap_e = (categoria_macro_key == "hospitales") | (nivel_num %in% c(2L, 3L)),
    ap_c = (categoria_macro_key == "hospitales") | (nivel_num %in% c(3L))
  ) %>%
  rowwise() %>%
  mutate(
    meta_tbl_ok = list({
      if (is.null(meta_tbl) || length(meta_tbl) == 0) {
        tibble::tibble(
          clues_imb = clues_imb,
          meta_general_anual = 0,
          meta_especialidad_anual = 0,
          meta_cirugia_anual = 0,
          meta_egresos_anual = 0
        )
      } else {
        as_tibble(meta_tbl)
      }
    }),
    
    mg_num  = fmt_int(meta_tbl_ok$meta_general_anual[1]),
    me_num  = fmt_int(meta_tbl_ok$meta_especialidad_anual[1]),
    mc_num  = fmt_int(meta_tbl_ok$meta_cirugia_anual[1]),
    meg_num = fmt_int(meta_tbl_ok$meta_egresos_anual[1]),
    
    tabla_productividad = paste0(
      "<div style='font-weight:900; margin-top:10px; margin-bottom:2px;'>Productividad</div>",
      "<div style='font-size:10px; color:#6b7280; margin-bottom:6px;'>", fecha_corte_lbl, "</div>",
      "<table style='width:100%; border-collapse:collapse; font-size:10.5px;'>",
      "<thead><tr>",
      "<th style='text-align:left; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Año</th>",
      "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Total</th>",
      "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>General</th>",
      "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Especialidad</th>",
      "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Qx</th>",
      "</tr></thead><tbody>",
      
      "<tr>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>2026</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(total_2026, TRUE), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(general_2026, TRUE), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(especialidad_2026, ap_e), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(qx_2026, ap_c), "</td>",
      "</tr>",
      
      "<tr>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>2025</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(total_2025, TRUE), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(general_2025, TRUE), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(especialidad_2025, ap_e), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(qx_2025, ap_c), "</td>",
      "</tr>",
      
      "<tr>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>2024</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(total_2024, TRUE), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(general_2024, TRUE), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(especialidad_2024, ap_e), "</td>",
      "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", fmt_val_aplica(qx_2024, ap_c), "</td>",
      "</tr>",
      
      "</tbody></table>"
    ),
    
    tabla_metas = {
      mg  <- fmt_val_aplica(mg_num, TRUE)
      me  <- fmt_val_aplica(me_num, ap_e)
      mc  <- fmt_val_aplica(mc_num, ap_c)
      meg <- fmt_val_aplica(meg_num, TRUE)
      
      ag  <- fmt_val_aplica(general_2026, TRUE)
      ae  <- fmt_val_aplica(especialidad_2026, ap_e)
      ac  <- fmt_val_aplica(qx_2026, ap_c)
      
      pg  <- fmt_pct_aplica(general_2026, mg_num, TRUE)
      pe  <- fmt_pct_aplica(especialidad_2026, me_num, ap_e)
      pc  <- fmt_pct_aplica(qx_2026, mc_num, ap_c)
      
      aeg <- "sin información"
      peg <- "sin información"
      
      paste0(
        "<div style='font-weight:900; margin-top:10px; margin-bottom:2px;'>Metas anuales</div>",
        "<div style='font-size:10px; color:#6b7280; margin-bottom:6px;'>", fecha_corte_lbl, " ", anio_corte, "</div>",
        "<table style='width:100%; border-collapse:collapse; font-size:10.5px;'>",
        "<thead><tr>",
        "<th style='text-align:left; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Indicador</th>",
        "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Meta</th>",
        "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>Avance</th>",
        "<th style='text-align:right; border-bottom:1px solid #e5e7eb; padding:6px 4px;'>% avance</th>",
        "</tr></thead><tbody>",
        
        "<tr>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>Consulta general</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", mg, "</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", ag, "</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", pg, "</td>",
        "</tr>",
        
        "<tr>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>Especialidad</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", me, "</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", ae, "</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", pe, "</td>",
        "</tr>",
        
        "<tr>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>Procedimientos quirúrgicos</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", mc, "</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", ac, "</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", pc, "</td>",
        "</tr>",
        
        "<tr>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; font-weight:800;'>Egresos</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", meg, "</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", aeg, "</td>",
        "<td style='padding:5px 4px; border-bottom:1px solid #f3f4f6; text-align:right;'>", peg, "</td>",
        "</tr>",
        
        "</tbody></table>"
      )
    },
    
    popup_text = paste0(
      "<div style='font-family: Arial, sans-serif; font-size: 11px; padding:0;'>",
      "<div style='background:", color_estatus, "; color:white; padding:8px 10px; border-radius:10px 10px 0 0;'>",
      "<div style='font-size:12px; font-weight:900;'>", nombre_de_la_unidad, "</div>",
      "<div style='font-size:10px; opacity:0.95;'>", estatus_simple, " · ", as.character(macro_label), "</div>",
      "</div>",
      "<div class='popup-scroll' style='padding:10px 10px; border:1px solid #e5e7eb; border-top:none; border-radius:0 0 10px 10px; line-height:1.35;'>",
      "<b>CLUES IMB:</b> ", clues_imb, "<br>",
      "<b>Entidad:</b> ", entidad, "<br>",
      "<b>Municipio:</b> ", municipio, "<br>",
      "<b>Localidad:</b> ", localidad, "<br>",
      "<b>Categoría gerencial (original):</b> ", categoria_clean, "<br>",
      "<b>Grupo:</b> ", as.character(macro_label), "<br>",
      "<b>Región:</b> ", nombre_region, "<br>",
      tabla_metas,
      tabla_productividad,
      tabla_brecha,
      tabla_infra,
      "</div></div>"
    )
  ) %>%
  ungroup() %>%
  select(-meta_tbl_ok, -mg_num, -me_num, -mc_num, -meg_num)

# =========================
# 6) DATALIST
# =========================
opciones_busqueda <- sort(unique(clues_mapa$search_label))
datalist_busqueda <- tags$datalist(
  id = "cluesList",
  lapply(opciones_busqueda, function(x) tags$option(value = x))
)

# =========================
# 7) MAPA
# =========================
mapa <- leaflet() %>%
  addMapPane("pane_otros_halo",             zIndex = 410) %>%
  addMapPane("pane_otros",                  zIndex = 411) %>%
  addMapPane("pane_unidades_moviles_halo",  zIndex = 420) %>%
  addMapPane("pane_unidades_moviles",       zIndex = 421) %>%
  addMapPane("pane_clinicas_halo",          zIndex = 430) %>%
  addMapPane("pane_clinicas",               zIndex = 431) %>%
  addMapPane("pane_hospitales_halo",        zIndex = 440) %>%
  addMapPane("pane_hospitales",             zIndex = 441) %>%
  addProviderTiles(providers$CartoDB.Positron, group = "Mapa gris") %>%
  addProviderTiles(providers$Esri.WorldImagery, group = "Satélite") %>%
  setView(lng=-102.5528, lat=23.6345, zoom=5)

add_markers_combo <- function(map, df, estatus, macro_key, group_name){
  d <- df %>% filter(estatus_simple == estatus, categoria_macro_key == macro_key)
  if(nrow(d) == 0) return(map)
  
  map <- map %>% addCircleMarkers(
    data = d,
    lng = ~longitud, lat = ~latitud,
    popup = NULL,
    label = NULL,
    group   = paste0(group_name, "|halo"),
    layerId = ~paste0(clues_imb, "_halo"),
    options = pathOptions(
      pane      = ~pane_halo,
      macro_key = ~categoria_macro_key,
      size_mult = ~size_mult
    ),
    color   = "#FFFFFF",
    weight  = 1.0,
    opacity = 0.95,
    fillColor   = "#FFFFFF",
    fillOpacity = 0.05,
    radius = 6.2,
    stroke = TRUE
  )
  
  map %>% addCircleMarkers(
    data = d,
    lng = ~longitud, lat = ~latitud,
    popup = ~popup_text,
    label = ~search_label,
    group = group_name,
    layerId = ~clues_imb,
    options = pathOptions(
      pane      = ~pane_main,
      macro_key = ~categoria_macro_key,
      size_mult = ~size_mult
    ),
    color = ~macro_border,
    weight = 0.7,
    opacity = 0.85,
    fillColor = ~macro_color,
    fillOpacity = 0.50,
    radius = 5.0,
    stroke = TRUE,
    popupOptions = popupOptions(maxWidth = 420)
  )
}

for (k in macro_keys) {
  mapa <- mapa %>%
    add_markers_combo(clues_mapa, "EN OPERACION", k, paste0("EN|", k)) %>%
    add_markers_combo(clues_mapa, "FUERA DE OPERACION", k, paste0("FUERA|", k))
}

mapa <- mapa %>%
  addLegend(
    position = "bottomright",
    colors   = macro_map$macro_color,
    labels   = macro_map$macro_label,
    title    = "Categoría gerencial",
    opacity  = 1
  )

mapa$elementId <- "mapa_widget"

# =========================
# 8) CSS Dashboard + MODALES
# =========================
css_dashboard <- tags$style(HTML(paste0("
  body{ margin:0; background:#f4f4f4; font-family: Arial, sans-serif; }

  .header{
    background:", COL_BLANCO, ";
    border-bottom:4px solid ", COL_VINO, ";
    padding:12px 16px;
    display:flex; align-items:center; justify-content:space-between;
  }
  .header .title{ font-size:22px; font-weight:900; color:", COL_VINO, "; }
  .header .sub{ margin-top:4px; font-size:12px; color:#444; }
  .header img{ height:44px; }

  .cards{
    display:grid;
    grid-template-columns: repeat(4, 1fr);
    gap:10px;
    padding:10px 12px;
  }
  .card{
    background:", COL_BLANCO, ";
    border:1px solid #e6e6e6;
    border-radius:12px;
    box-shadow:0 2px 10px rgba(0,0,0,0.06);
    padding:10px 12px;
  }
  .card .k{ font-size:12px; font-weight:800; color:#555; }
  .card .v{ font-size:24px; font-weight:900; color:", COL_VINO, "; margin-top:6px; }
  .card .bar{ height:6px; border-radius:999px; background:", COL_DORADO, "; margin-top:8px; }

  .main{
    display:grid;
    grid-template-columns: 280px 1fr;
    gap:10px;
    padding:0 12px 12px 12px;
  }
  .sidebar{
    background:", COL_BLANCO, ";
    border:1px solid #e6e6e6;
    border-radius:12px;
    box-shadow:0 2px 10px rgba(0,0,0,0.06);
    padding:12px;
    height: calc(100vh - 210px);
    overflow:auto;
  }
  .mapwrap{
    background:", COL_BLANCO, ";
    border:1px solid #e6e6e6;
    border-radius:12px;
    box-shadow:0 2px 10px rgba(0,0,0,0.06);
    padding:10px;
    height: calc(100vh - 210px);
  }

  #mapa_widget{ height:100% !important; width:100% !important; }
  .leaflet-container{ height:100% !important; width:100% !important; border-radius:10px; }

  .sec{
    font-size:12px;
    font-weight:900;
    color:", COL_VINO, ";
    letter-spacing:0.6px;
    text-transform:uppercase;
    margin:10px 0 6px 0;
  }
  input[type=text]{
    width:100%;
    padding:8px 10px;
    border:1px solid #ddd;
    border-radius:10px;
    outline:none;
  }
  .btn{
    width:100%;
    padding:9px 10px;
    border:none;
    border-radius:10px;
    background:", COL_VINO, ";
    color:white;
    font-weight:800;
    cursor:pointer;
  }
  .btn:hover{ background:#641824; }

  .pill{ display:flex; gap:8px; flex-wrap:wrap; }
  .pill label{
    display:flex; align-items:center; gap:6px;
    border:1px solid #ddd;
    border-radius:999px;
    padding:6px 10px;
    cursor:pointer;
    user-select:none;
    font-size:12px;
  }

  .highlight-pin{
    width:26px; height:26px; border-radius:50%;
    background:", COL_DORADO, ";
    border:3px solid #000;
    box-shadow:0 2px 8px rgba(0,0,0,0.35);
    display:flex; align-items:center; justify-content:center;
    font-size:16px; font-weight:900;
  }

  .popup-scroll{
    max-height: 320px;
    overflow-y: auto;
  }
  .popup-scroll::-webkit-scrollbar{ width: 10px; }
  .popup-scroll::-webkit-scrollbar-thumb{
    background: rgba(0,0,0,0.18);
    border-radius: 999px;
  }
  .popup-scroll::-webkit-scrollbar-track{
    background: rgba(0,0,0,0.05);
    border-radius: 999px;
  }

  .modal-overlay{
    position:fixed;
    inset:0;
    background: rgba(0,0,0,0.55);
    z-index: 999999;
    display:none;
    align-items:center;
    justify-content:center;
    padding: 18px;
  }
  .modal-overlay.show{ display:flex; }

  .modal-box{
    width: min(1200px, 96vw);
    height: min(780px, 92vh);
    background:#fff;
    border-radius:16px;
    box-shadow:0 18px 50px rgba(0,0,0,0.30);
    overflow:hidden;
    display:flex;
    flex-direction:column;
    border: 1px solid rgba(0,0,0,0.10);
  }
  .modal-topbar{
    background:", COL_VINO, ";
    color:#fff;
    display:flex;
    align-items:center;
    justify-content:space-between;
    padding: 10px 12px;
    border-bottom: 4px solid ", COL_DORADO, ";
  }
  .modal-title{
    font-weight:900;
    font-size:13px;
    letter-spacing:0.4px;
    text-transform:uppercase;
  }
  .modal-close{
    background: rgba(255,255,255,0.15);
    border: 1px solid rgba(255,255,255,0.35);
    color:#fff;
    border-radius:10px;
    padding:8px 10px;
    font-weight:900;
    cursor:pointer;
  }
  .modal-close:hover{ background: rgba(255,255,255,0.25); }

  .modal-iframe{
    flex:1;
    width:100%;
    border:0;
    background:#fff;
  }

  .info-modal-overlay{
    position:fixed;
    inset:0;
    background: rgba(0,0,0,0.45);
    z-index: 1000000;
    display:none;
    align-items:center;
    justify-content:center;
    padding: 18px;
  }
  .info-modal-overlay.show{ display:flex; }

  .info-modal-box{
    width:min(460px, 94vw);
    background:#fff;
    border-radius:16px;
    box-shadow:0 18px 50px rgba(0,0,0,0.28);
    overflow:hidden;
    border:1px solid rgba(0,0,0,0.10);
  }
  .info-modal-head{
    background:", COL_VINO, ";
    color:#fff;
    padding:12px 14px;
    font-weight:900;
    font-size:13px;
    letter-spacing:0.4px;
    text-transform:uppercase;
    border-bottom:4px solid ", COL_DORADO, ";
  }
  .info-modal-body{
    padding:18px 16px 10px 16px;
    font-size:14px;
    color:#374151;
    line-height:1.5;
  }
  .info-modal-actions{
    padding:0 16px 16px 16px;
  }
")))

# =========================
# 9) Cards
# =========================
nt <- nrow(clues_mapa)
conteo_macro <- clues_mapa %>% count(categoria_macro_key)
n_hos <- conteo_macro$n[conteo_macro$categoria_macro_key == "hospitales"]; if(length(n_hos)==0) n_hos <- 0
n_cli <- conteo_macro$n[conteo_macro$categoria_macro_key == "clinicas"]; if(length(n_cli)==0) n_cli <- 0
n_mov <- conteo_macro$n[conteo_macro$categoria_macro_key == "unidades_moviles"]; if(length(n_mov)==0) n_mov <- 0
n_otr <- conteo_macro$n[conteo_macro$categoria_macro_key == "otros"]; if(length(n_otr)==0) n_otr <- 0

header_html <- HTML(paste0('
  <div class="header">
    <div>
      <div class="title">Reporte Integral de productividad y personal</div>
      <div class="sub">IMSS-BIENESTAR · Servicios Públicos de Salud</div>
    </div>
    <img src="imagenes/logo_imssb.png" />
  </div>

  <div class="cards">
    <div class="card"><div class="k">CLUES TOTALES</div><div class="v"><span id="vb_total">', nt, '</span></div><div class="bar"></div></div>
    <div class="card"><div class="k">HOSPITALES</div><div class="v"><span id="vb_hos">', n_hos, '</span></div><div class="bar"></div></div>
    <div class="card"><div class="k">CLÍNICAS</div><div class="v"><span id="vb_cli">', n_cli, '</span></div><div class="bar"></div></div>
    <div class="card"><div class="k">U. MÓVILES / OTROS</div><div class="v"><span id="vb_movotr">', (n_mov + n_otr), '</span></div><div class="bar"></div></div>
  </div>
'))

cats_checks_html <- paste0(
  "<div class=\"pill\" style=\"margin-bottom:10px;\">",
  paste0(
    "<label><input type=\"checkbox\" class=\"cat\" value=\"", macro_map$macro_key, "\" checked> ",
    macro_map$macro_label,
    "</label>",
    collapse = ""
  ),
  "</div>"
)

sidebar_html <- tagList(
  HTML(paste0('
    <div class="sec">Buscador</div>
    <div style="margin-bottom:8px;">
      <input id="q" type="text" list="cluesList"
             placeholder="Buscar por CLUES o nombre de la unidad" />
    </div>
    <div style="margin-bottom:10px;">
      <button class="btn" id="btnBuscar">Buscar</button>
    </div>

    <div class="sec">Filtro estatus</div>
    <div class="pill" style="margin-bottom:10px;">
      <label><input type="checkbox" id="op_on" checked> EN OPERACION</label>
      <label><input type="checkbox" id="op_off"> FUERA DE OPERACION</label>
    </div>

    <div class="sec">Filtro categoría</div>
    ', cats_checks_html, '

    <div class="sec">Tipo de mapa</div>
    <div class="pill" style="margin-bottom:10px;">
      <label><input type="radio" name="basemap" value="std" checked> Gris</label>
      <label><input type="radio" name="basemap" value="sat"> Satélite</label>
    </div>

    <div class="sec">Acciones</div>
    <div>
      <button class="btn" id="btnReset">Restablecer vista</button>
    </div>

    <div class="doc-card" style="margin-top:12px;">
      <div class="doc-title" style="font-weight:900; color:', COL_VINO, ';">MÓDULO DE DOCUMENTOS</div>
      <button class="btn" id="btnDocs">Abrir generador (PPTX/XLSX)</button>
      <div class="doc-note" style="margin-top:8px; font-size:11px; color:#666;">
        Se abre dentro de esta misma pantalla (modal embebido).
      </div>
    </div>
  '))
)

# =========================
# 10) JS
# =========================
js_dashboard <- tags$script(HTML(paste0("
(function(){
  function ready(fn){
    if(document.readyState !== 'loading') fn();
    else document.addEventListener('DOMContentLoaded', fn);
  }
  function norm(x){
    return String(x || '')
      .toLowerCase()
      .normalize('NFD').replace(/\\p{Diacritic}/gu,'')
      .trim();
  }

  ready(function(){

    // ===== MODAL DOCS =====
    var modal = document.getElementById('docsModal');
    var iframe = document.getElementById('docsFrame');
    var btnDocs = document.getElementById('btnDocs');
    var btnClose = document.getElementById('btnCloseDocs');

    function openDocs(){
      if(!modal || !iframe) return;
      iframe.src = '", DOCS_URL, "';
      modal.classList.add('show');
    }
    function closeDocs(){
      if(!modal || !iframe) return;
      modal.classList.remove('show');
      iframe.src = 'about:blank';
    }

    if(btnDocs){ btnDocs.addEventListener('click', function(e){ e.preventDefault(); openDocs(); }); }
    if(btnClose){ btnClose.addEventListener('click', function(e){ e.preventDefault(); closeDocs(); }); }
    if(modal){ modal.addEventListener('click', function(e){ if(e.target === modal) closeDocs(); }); }
    document.addEventListener('keydown', function(e){ if(e.key === 'Escape') closeDocs(); });

    // ===== MODAL INFO BUSCADOR =====
    var infoModal = document.getElementById('searchInfoModal');
    var infoText  = document.getElementById('searchInfoText');
    var btnInfoClose = document.getElementById('btnCloseSearchInfo');

    function openSearchInfo(msg){
      if(!infoModal || !infoText) return;
      infoText.textContent = msg || '';
      infoModal.classList.add('show');
    }
    function closeSearchInfo(){
      if(!infoModal) return;
      infoModal.classList.remove('show');
    }

    if(btnInfoClose){
      btnInfoClose.addEventListener('click', function(e){
        e.preventDefault();
        closeSearchInfo();
      });
    }
    if(infoModal){
      infoModal.addEventListener('click', function(e){
        if(e.target === infoModal) closeSearchInfo();
      });
    }

    // ===== MAPA =====
    var w = HTMLWidgets.find('#mapa_widget');
    if(!w){ console.warn('No se encontró #mapa_widget'); return; }
    var map = w.getMap();

    var baseStd = null, baseSat = null;
    map.eachLayer(function(layer){
      if(layer && (layer instanceof L.TileLayer) && layer._url){
        var u = String(layer._url).toLowerCase();
        if(u.includes('carto') || u.includes('basemaps.cartocdn.com') || u.includes('positron') || u.includes('light_all') || u.includes('light_nolabels')) baseStd = layer;
        if(u.includes('world_imagery') || u.includes('worldimagery')) baseSat = layer;
      }
    });

    var groups = {};
    var circlesMain = [];
    var circlesHalo = [];

    map.eachLayer(function(layer){
      if(layer && (layer instanceof L.CircleMarker) && layer.options){
        var gid = (layer.options.group || '');
        var lid = (layer.options.layerId || '');
        var isHalo = String(gid).indexOf('|halo') !== -1 || String(lid).endsWith('_halo');
        if(isHalo) circlesHalo.push(layer);
        else circlesMain.push(layer);
      }
      if(layer && (layer instanceof L.CircleMarker) && layer.options && layer.options.group){
        var g = layer.options.group;
        if(!groups[g]) groups[g] = [];
        groups[g].push(layer);
      }
    });

    function radiusForZoom(z){
      if(z <= 5)  return 1.0;
      if(z <= 6)  return 1.6;
      if(z <= 7)  return 2.4;
      if(z <= 8)  return 3.4;
      if(z <= 9)  return 4.8;
      if(z <= 10) return 6.6;
      if(z <= 11) return 8.6;
      if(z <= 12) return 11.0;
      if(z <= 13) return 13.8;
      return 16.5;
    }

    function multOf(layer){
      var m = 1.0;
      try {
        if(layer && layer.options && layer.options.size_mult != null){
          m = parseFloat(layer.options.size_mult);
          if(!isFinite(m) || m <= 0) m = 1.0;
        }
      } catch(e){ m = 1.0; }
      return m;
    }

    function applyRadius(){
      var z = map.getZoom();
      var r = radiusForZoom(z);

      for(var i=0; i<circlesMain.length; i++){
        try {
          var m = multOf(circlesMain[i]);
          circlesMain[i].setRadius(r * m);
        } catch(e){}
      }

      var rh = r + 1.1;
      for(var j=0; j<circlesHalo.length; j++){
        try {
          var mh = multOf(circlesHalo[j]);
          circlesHalo[j].setRadius(rh * mh);
        } catch(e){}
      }
    }
    map.on('zoomend', applyRadius);

    function updateValueBoxes(){
      var total = 0, hos = 0, cli = 0, mov = 0, otr = 0;

      for(var i=0; i<circlesMain.length; i++){
        var lyr = circlesMain[i];
        if(!map.hasLayer(lyr)) continue;

        total++;

        var mk = (lyr.options && lyr.options.macro_key) ? String(lyr.options.macro_key) : 'otros';
        if(mk === 'hospitales') hos++;
        else if(mk === 'clinicas') cli++;
        else if(mk === 'unidades_moviles') mov++;
        else otr++;
      }

      var elTotal  = document.getElementById('vb_total');
      var elHos    = document.getElementById('vb_hos');
      var elCli    = document.getElementById('vb_cli');
      var elMovOtr = document.getElementById('vb_movotr');

      if(elTotal)  elTotal.textContent  = total;
      if(elHos)    elHos.textContent    = hos;
      if(elCli)    elCli.textContent    = cli;
      if(elMovOtr) elMovOtr.textContent = (mov + otr);
    }

    var highlightMarker = null;
    var hiIcon = L.divIcon({
      className: '',
      html: '<div class=\"highlight-pin\">★</div>',
      iconSize: [26, 26],
      iconAnchor: [13, 13]
    });

    function clearHighlight(){
      if(highlightMarker){
        map.removeLayer(highlightMarker);
        highlightMarker = null;
      }
    }

    function selectedEstatus(){
      return {
        en: document.getElementById('op_on').checked,
        fuera: document.getElementById('op_off').checked
      };
    }

    function selectedCats(){
      var set = {};
      var checks = document.querySelectorAll('input.cat');
      for(var i=0; i<checks.length; i++){
        if(checks[i].checked) set[checks[i].value] = true;
      }
      return set;
    }

    function showGroup(gname, visible){
      var arr = groups[gname] || [];
      for(var i=0; i<arr.length; i++){
        var lyr = arr[i];
        if(visible){
          if(!map.hasLayer(lyr)) lyr.addTo(map);
        } else {
          if(map.hasLayer(lyr)) map.removeLayer(lyr);
        }
      }
    }

    function applyFilters(){
      clearHighlight();
      var e = selectedEstatus();
      var cats = selectedCats();

      Object.keys(groups).forEach(function(g){
        var parts = g.split('|');
        if(parts.length < 2) return;
        var est = parts[0];
        var macro = parts[1];

        var okE = (est === 'EN' && e.en) || (est === 'FUERA' && e.fuera);
        var okC = !!cats[macro];
        showGroup(g, okE && okC);
      });

      applyRadius();
      updateValueBoxes();
    }

    function setBasemap(which){
      if(baseStd && map.hasLayer(baseStd)) map.removeLayer(baseStd);
      if(baseSat && map.hasLayer(baseSat)) map.removeLayer(baseSat);
      if(which === 'std' && baseStd) baseStd.addTo(map);
      if(which === 'sat' && baseSat) baseSat.addTo(map);
    }

    function matchLayer(layer, candidate, rawNorm){
      if(!layer || !(layer instanceof L.CircleMarker)) return false;

      var gid = String(layer.options.group || '');
      var lid = String(layer.options.layerId || '');
      if(gid.indexOf('|halo') !== -1 || lid.endsWith('_halo')) return false;

      var id = layer.options.layerId ? norm(layer.options.layerId) : '';
      var popupHtml = (layer.getPopup && layer.getPopup()) ? norm(layer.getPopup().getContent()) : '';

      return (id && id === candidate) ||
             (id && id.includes(candidate)) ||
             (popupHtml && popupHtml.includes(rawNorm));
    }

    function doSearch(){
      var raw = (document.getElementById('q').value || '').trim();
      if(!raw) return;

      closeSearchInfo();
      clearHighlight();

      var rawNorm = norm(raw);
      var candidate = rawNorm;
      if(rawNorm.includes('|')) candidate = norm(rawNorm.split('|')[0]);

      var foundVisible = null;
      var foundHidden  = null;

      for(var i=0; i<circlesMain.length; i++){
        var layer = circlesMain[i];
        if(matchLayer(layer, candidate, rawNorm)){
          if(map.hasLayer(layer)){
            foundVisible = layer;
            break;
          } else if(!foundHidden) {
            foundHidden = layer;
          }
        }
      }

      if(foundVisible){
        var ll = foundVisible.getLatLng();
        map.setView([ll.lat, ll.lng], 14);
        highlightMarker = L.marker(ll, { icon: hiIcon, interactive: false }).addTo(map);
        if(foundVisible.openPopup) foundVisible.openPopup();
        applyRadius();
        return;
      }

      if(foundHidden){
        var hiddenGroup = String(foundHidden.options.group || '');
        var isFuera = hiddenGroup.indexOf('FUERA|') === 0;
        var opOffChecked = document.getElementById('op_off').checked;

        if(isFuera && !opOffChecked){
          openSearchInfo('Esta unidad está fuera de operación, marcar la casilla Fuera de operación para mostrarla.');
          return;
        } else {
          openSearchInfo('La unidad encontrada no está visible con los filtros actuales.');
          return;
        }
      }
    }

    document.getElementById('op_on').addEventListener('change', applyFilters);
    document.getElementById('op_off').addEventListener('change', applyFilters);

    var catChecks = document.querySelectorAll('input.cat');
    for(var i=0; i<catChecks.length; i++){
      catChecks[i].addEventListener('change', applyFilters);
    }

    var radios = document.querySelectorAll('input[name=basemap]');
    for(var r=0; r<radios.length; r++){
      radios[r].addEventListener('change', function(){ setBasemap(this.value); });
    }

    document.getElementById('btnReset').addEventListener('click', function(){
      closeSearchInfo();
      clearHighlight();
      map.setView([23.6345, -102.5528], 5);
      applyRadius();
      updateValueBoxes();
    });

    document.getElementById('btnBuscar').addEventListener('click', doSearch);
    document.getElementById('q').addEventListener('keydown', function(e){
      if(e.key === 'Enter') doSearch();
    });

    setBasemap('std');
    applyFilters();

    setTimeout(function(){ map.invalidateSize(); applyRadius(); updateValueBoxes(); }, 400);
    setTimeout(function(){ map.invalidateSize(); applyRadius(); updateValueBoxes(); }, 1200);
  });
})();
")))

# =========================
# 11) ARMAR PÁGINA HTML
# =========================
page <- tagList(
  tags$html(
    tags$head(css_dashboard),
    tags$body(
      datalist_busqueda,
      header_html,
      div(
        class = "main",
        div(class = "sidebar", sidebar_html),
        div(class = "mapwrap", mapa)
      ),
      
      HTML(paste0("
        <div id='docsModal' class='modal-overlay' aria-hidden='true'>
          <div class='modal-box' role='dialog' aria-modal='true'>
            <div class='modal-topbar'>
              <div class='modal-title'>Módulo de documentos</div>
              <button id='btnCloseDocs' class='modal-close' type='button'>Cerrar ✕</button>
            </div>
            <iframe id='docsFrame' class='modal-iframe' src='about:blank'></iframe>
          </div>
        </div>
      ")),
      
      HTML(paste0("
        <div id='searchInfoModal' class='info-modal-overlay' aria-hidden='true'>
          <div class='info-modal-box' role='dialog' aria-modal='true'>
            <div class='info-modal-head'>Aviso</div>
            <div class='info-modal-body' id='searchInfoText'></div>
            <div class='info-modal-actions'>
              <button id='btnCloseSearchInfo' class='btn' type='button'>Entendido</button>
            </div>
          </div>
        </div>
      ")),
      
      js_dashboard
    )
  )
)

browsable(page)

htmltools::save_html(
  page,
  file = "C:/Users/josue.morales/Desktop/HTML mapa/index.html"
)