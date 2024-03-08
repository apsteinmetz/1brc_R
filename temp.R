bench <- function (..., list = NULL, times = 100L, unit = NULL, check = NULL,
          control = list(), setup = NULL)
{
   stopifnot(times == as.integer(times))
   if (!missing(unit) && !is.null(unit))
      stopifnot(is.character(unit), length(unit) == 1L)
   unit <- "seconds"
   control[["warmup"]] <- coalesce(control[["warmup"]], 2^18L)
   control[["order"]] <- coalesce(control[["order"]], "random")
   stopifnot(as.integer(control$warmup) == control$warmup)
   exprs <- c(as.list(match.call(expand.dots = FALSE)$...),
              list)
   nm <- names(exprs)
   exprnm <- sapply(exprs, function(e) paste(deparse(e), collapse = " "))
   if (is.null(nm))
      nm <- exprnm
   else nm[nm == ""] <- exprnm[nm == ""]
   names(exprs) <- nm
   env <- parent.frame()
   setup <- substitute(setup)
   if (!is.null(check)) {
      setupexpr <- as.expression(setup)
      checkexprs <- lapply(exprs, function(e) c(setupexpr,
                                                e))
      values <- lapply(checkexprs, eval, env)
      if (is.character(check) && isTRUE(check == "equal")) {
         check <- function(values) {
            all(sapply(values[-1], function(x) isTRUE(all.equal(values[[1]],
                                                                x))))
         }
      }
      else if (is.character(check) && isTRUE(check == "equivalent")) {
         check <- function(values) {
            all(sapply(values[-1], function(x) isTRUE(all.equal(values[[1]],
                                                                x, check.attributes = F))))
         }
      }
      else if (is.character(check) && isTRUE(check == "identical")) {
         check <- function(values) {
            all(sapply(values[-1], function(x) identical(values[[1]],
                                                         x)))
         }
      }
      ok <- check(values)
      if (!isTRUE(ok)) {
         stop("Input expressions are not equivalent.", call. = FALSE)
      }
   }
   gc(FALSE)
   o <- if (control$order == "random")
      sample(rep(seq_along(exprs), times = times))
   else if (control$order == "inorder")
      rep(seq_along(exprs), times = times)
   else if (control$order == "block")
      rep(seq_along(exprs), each = times)
   else stop("Unknown ordering. Must be one of 'random', 'inorder' or 'block'.")
   exprs <- exprs[o]
   if (anyDuplicated(nm) > 0) {
      duplicates <- nm[duplicated(nm)]
      stop("Expression names must be unique. Duplicate expression names: ",
           paste(duplicates, collapse = ", "))
   }
   expr <- factor(nm[o], levels = nm)
   res <- 1:length(exprs)
   if (all(is.na(res)))
      .all_na_stop()
   res <- data.frame(expr = expr, time = res)
   class(res) <- c("microbenchmark", class(res))
   if (!is.null(unit))
      attr(res, "unit") <- unit
   res
}

bench(do_dplyr)
