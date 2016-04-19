resource "aws_dynamodb_table" "nps_survey" {
    name           = "${var.environment}_nps_survey"
    read_capacity  = "${var.nps_survey_read_capacity}"
    write_capacity = "${var.nps_survey_write_capacity}"
    hash_key       = "agency_id"
    range_key      = "profile_id"
    attribute {
      name = "agency_id"
      type = "N"
    }
    attribute {
      name = "profile_id"
      type = "N"
    }
    /*attribute {*/
      /*name = "recommend_score"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "comments"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "favorite"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "change"*/
      /*type = "S"*/
    /*}*/
}

resource "aws_dynamodb_table" "nps_survey_2015_q1" {
    name           = "${var.environment}_nps_survey_2015_q1"
    read_capacity  = "${var.nps_survey_2015_q1_read_capacity}"
    write_capacity = "${var.nps_survey_2015_q1_write_capacity}"
    hash_key       = "agency_id"
    range_key      = "profile_id"
    attribute {
      name = "agency_id"
      type = "N"
    }
    attribute {
      name = "profile_id"
      type = "N"
    }
    /*attribute {*/
      /*name = "recommend_score"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "comments"*/
      /*type = "S"*/
    /*}*/
}

resource "aws_dynamodb_table" "nps_survey_2015_q3" {
    name           = "${var.environment}_nps_survey_2015_q3"
    read_capacity  = "${var.nps_survey_2015_q3_read_capacity}"
    write_capacity = "${var.nps_survey_2015_q3_write_capacity}"
    hash_key       = "agency_id"
    range_key      = "profile_id"
    attribute {
      name = "agency_id"
      type = "N"
    }
    attribute {
      name = "profile_id"
      type = "N"
    }
    /*attribute {*/
      /*name = "recommend_score"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "comments"*/
      /*type = "S"*/
    /*}*/
}

resource "aws_dynamodb_table" "nps_survey_2015_q4" {
    name           = "${var.environment}_nps_survey_2015_q4"
    read_capacity  = "${var.nps_survey_2015_q4_read_capacity}"
    write_capacity = "${var.nps_survey_2015_q4_write_capacity}"
    hash_key       = "agency_id"
    range_key      = "profile_id"
    attribute {
      name = "agency_id"
      type = "N"
    }
    attribute {
      name = "profile_id"
      type = "N"
    }
    /*attribute {*/
      /*name = "recommend_score"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "comments"*/
      /*type = "S"*/
    /*}*/
}

resource "aws_dynamodb_table" "wc_renewal" {
    name           = "${var.environment}_wc_renewal"
    read_capacity  = "${var.wc_renewal_read_capacity}"
    write_capacity = "${var.wc_renewal_write_capacity}"
    hash_key       = "agency_id"
    range_key      = "profile_id"
    attribute {
      name = "agency_id"
      type = "N"
    }
    attribute {
      name = "profile_id"
      type = "N"
    }
    /*attribute {*/
      /*name = "renewal_month"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "created"*/
      /*type = "N"*/
    /*}*/
}

resource "aws_dynamodb_table" "telephony_call_logs" {
    name           = "${var.environment}_telephony_call_logs"
    read_capacity  = "${var.telephony_call_logs_read_capacity}"
    write_capacity = "${var.telephony_call_logs_write_capacity}"
    hash_key       = "session_id"
    range_key      = "time"
    attribute {
      name = "session_id"
      type = "N"
    }
    attribute {
      name = "time"
      type = "N"
    }
    /*attribute {*/
      /*name = "direction"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "contents"*/
      /*type = "S"*/
    /*}*/
}

resource "aws_dynamodb_table" "change_in_condition" {
    name                     = "${var.environment}_change_in_condition"
    read_capacity            = "10"
    write_capacity           = "10"
    hash_key                 = "carelog_id"
    range_key                = "time"
    global_secondary_index {
      name            = "SavedInRDB"
      hash_key        = "saved_in_rdb"
      range_key       = "time"
      read_capacity   = "15"
      write_capacity  = "15"
      projection_type = "ALL"
    }
    local_secondary_index {
      name            = "SessionId"
      hash_key        = "carelog_id"
      range_key       = "session_id"
      projection_type = "ALL"
    }
    attribute {
      name = "carelog_id"
      type = "N"
    }
    attribute {
      name = "time"
      type = "N"
    }
    attribute {
      name = "saved_in_rdb"
      type = "N"
    }
    attribute {
      name = "session_id"
      type = "N"
    }
    /*attribute {*/
      /*name = "rdb_id"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "3_overall_differences"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "3_reduced_alertness"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "3_agitated_confused"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "3_pain"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "4_mobility"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "4_stand_walk_changed"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "4_fall_or_slip"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "5_eating_drinking_changed"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "6_toileting"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "6_urination_change"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "6_constipation"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "7_skin_condition_swelling"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "7_skin_rash_wound"*/
      /*type = "N"*/
    /*}*/
}

resource "aws_dynamodb_table" "conversion_report" {
    name           = "${var.environment}_conversion_report"
    read_capacity  = "${var.conversion_report_read_capacity}"
    write_capacity = "${var.conversion_report_write_capacity}"
    hash_key       = "time"
    range_key      = "agency_id"
    attribute {
      name = "time"
      type = "N"
    }
    attribute {
      name = "agency_id"
      type = "N"
    }
    /*attribute {*/
      /*name = "autopay_enabled"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "e_pay_account_id"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "e_pay_account_token"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "e_pay_activated"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "e_pay_enable"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "e_pay_merchant_id"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "e_pay_settings_confirmed"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "e_pay_terminal_id"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "name"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "num_clients"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "num_clients_with_e_payers"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "num_invoices"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "num_regular_payments"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "subdomain"*/
      /*type = "S"*/
    /*}*/
    /*attribute {*/
      /*name = "total_invoice_amount"*/
      /*type = "N"*/
    /*}*/
    /*attribute {*/
      /*name = "total_regular_payment_amount"*/
      /*type = "N"*/
    /*}*/
}

resource "aws_dynamodb_table" "es_conversion_report" {
    name           = "${var.environment}_es_conversion_report"
    read_capacity  = "${var.es_conversion_report_read_capacity}"
    write_capacity = "${var.es_conversion_report_write_capacity}"
    hash_key       = "time"
    range_key      = "subdomain"
    attribute {
      name = "time"
      type = "N"
    }
    attribute {
      name = "subdomain"
      type = "S"
    }
}

resource "aws_dynamodb_table" "es_wotc_pay_period" {
    name           = "${var.environment}_es_wotc_pay_period"
    read_capacity  = "${var.es_wotc_pay_period_read_capacity}"
    write_capacity = "${var.es_wotc_pay_period_write_capacity}"
    hash_key       = "period_start"
    range_key      = "profile_id"
    local_secondary_index {
      name            = "WotcPayPeriodAgency"
      range_key       = "agency_subdomain"
      projection_type = "ALL"
    }
    attribute {
      name = "period_start"
      type = "N"
    }
    attribute {
      name = "profile_id"
      type = "N"
    }
    attribute {
      name = "agency_subdomain"
      type = "S"
    }
}
