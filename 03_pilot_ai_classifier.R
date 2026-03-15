library(dplyr)
library(jsonlite)
library(purrr)
library(tibble)
library(readr)
library(stringr)

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

pilot_objects_lx <- readRDS("pilot_outputs/pilot_llm_objects.rds")
pilot_llm_dfx <- pilot_objects_lx$pilot_llm_dfx

stopifnot(all(c("row_id", "citation_text", "pilot_source") %in% names(pilot_llm_dfx)))

batch_input_path <- "pilot_outputs/gdc_batch_input_test2.jsonl"

clean_jsonl_text_x <- function(x) {
  x %>%
    enc2utf8() %>%
    iconv(from = "UTF-8", to = "UTF-8", sub = "") %>%
    str_replace_all("[\r\n]+", " ") %>%
    str_replace_all("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", " ") %>%
    str_squish()
}

instruction_x <- paste(
  "Classify the citation narrative as one of:",
  "FEDERAL_GDC, STATE_GDC, OSHA_STANDARD, or UNCERTAIN.",
  "FEDERAL_GDC = Section 5(a)(1), general duty clause, or 29 USC 654(a)(1).",
  "STATE_GDC = state-plan general duty analogue.",
  "OSHA_STANDARD = a specific OSHA or state safety standard is cited.",
  "If a specific standard is cited, classify as OSHA_STANDARD.",
  "Set is_gdc=true only for FEDERAL_GDC or STATE_GDC.",
  "For OSHA_STANDARD, provide the cited or most likely standard in standard_guess.",
  "If no specific standard can be identified, use null for standard_guess.",
  "Return JSON only:",
  '{"classification":"FEDERAL_GDC|STATE_GDC|OSHA_STANDARD|UNCERTAIN","is_gdc":true,"state_reference":true,"state_name":null,"standard_guess":null,"reason_short":"brief explanation"}'
) %>%
  clean_jsonl_text_x()

request_manifest_dfx <- pilot_llm_dfx %>%
  transmute(
    row_id,
    custom_id = paste0("row_", row_id),
    pilot_source,
    citation_text = clean_jsonl_text_x(citation_text)
  )

batch_requests_lx <- request_manifest_dfx %>%
  mutate(
    request = pmap(
      list(custom_id, citation_text),
      function(custom_id, citation_text) {
        list(
          custom_id = custom_id,
          method = "POST",
          url = "/v1/responses",
          body = list(
            model = "gpt-4.1",
            instructions = instruction_x,
            input = paste0("Citation narrative: ", citation_text),
            max_output_tokens = 120
          )
        )
      }
    )
  ) %>%
  pull(request)

json_lines_x <- map_chr(
  batch_requests_lx,
  ~ jsonlite::toJSON(.x, auto_unbox = TRUE, null = "null", pretty = FALSE)
)

# extra guardrail: no physical line breaks allowed in JSONL rows
bad_breaks_x <- which(str_detect(json_lines_x, "[\r\n]"))

if (length(bad_breaks_x) > 0) {
  stop(
    "Generated JSONL contains physical line breaks at line(s): ",
    paste(head(bad_breaks_x, 10), collapse = ", ")
  )
}

# validate each JSON object
bad_json_x <- which(!vapply(json_lines_x, jsonlite::validate, logical(1)))

if (length(bad_json_x) > 0) {
  stop(
    "Invalid JSON generated at line(s): ",
    paste(head(bad_json_x, 10), collapse = ", ")
  )
}

writeLines(json_lines_x, batch_input_path, useBytes = TRUE)

saveRDS(request_manifest_dfx, "pilot_outputs/request_manifest_dfx.rds")
write_csv(request_manifest_dfx, "pilot_outputs/request_manifest_dfx.csv")

message("Wrote valid JSONL to: ", batch_input_path)

# RUN SHELL


results_raw_x <- readLines("pilot_outputs/gdc_batch_input_test2_output_20260315T200907Z.jsonl")

results_parsed_dfx <- map_dfr(results_raw_x, function(line_x) {
  
  rec_x <- fromJSON(line_x, simplifyVector = FALSE)
  
  content_x <- rec_x$response$body$output[[1]]$content[[1]]$text %||% NA_character_
  
  parsed_x <- tryCatch(
    fromJSON(content_x),
    error = function(e) list(
      classification = "UNCERTAIN",
      is_gdc = NA,
      state_reference = NA,
      state_name = NA_character_,
      standard_guess = NA_character_,
      reason_short = paste("parse_error:", conditionMessage(e))
    )
  )
  
  tibble(
    custom_id = rec_x$custom_id %||% NA_character_,
    classification = parsed_x$classification %||% "UNCERTAIN",
    is_gdc = parsed_x$is_gdc %||% NA,
    state_reference = parsed_x$state_reference %||% NA,
    state_name = parsed_x$state_name %||% NA_character_,
    standard_guess = parsed_x$standard_guess %||% NA_character_,
    reason_short = parsed_x$reason_short %||% NA_character_
  )
}) %>%
  mutate(
    row_id = as.integer(sub("^row_", "", custom_id))
  )

pilot_openai_dfx <- pilot_llm_dfx %>%
  left_join(
    results_parsed_dfx %>%
      select(
        row_id,
        classification,
        is_gdc,
        state_reference,
        state_name,
        standard_guess,
        reason_short
      ),
    by = "row_id"
  )

saveRDS(pilot_openai_dfx, file = "pilot_outputs/pilot_openai_dfx.rds")
write_csv(pilot_openai_dfx, file = "pilot_outputs/pilot_openai_dfx.csv")
