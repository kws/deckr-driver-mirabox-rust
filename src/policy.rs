use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use regex::Regex;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Str(String),
    Bool(bool),
    None,
}

impl Value {
    fn as_bool(&self) -> Result<bool> {
        match self {
            Self::Bool(value) => Ok(*value),
            _ => bail!("expected boolean value"),
        }
    }

    fn as_string(&self) -> String {
        match self {
            Self::Int(value) => value.to_string(),
            Self::Str(value) => value.clone(),
            Self::Bool(value) => value.to_string(),
            Self::None => String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Value(Value),
    Identifier(String),
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    Compare {
        left: Box<Expr>,
        op: CompareOp,
        right: Box<Expr>,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Identifier(String),
    Number(i64),
    String(String),
    And,
    Or,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    LParen,
    RParen,
    Comma,
}

pub fn parse_expression(input: &str) -> Result<Expr> {
    let tokens = tokenize(input)?;
    let mut parser = Parser {
        tokens: &tokens,
        index: 0,
    };
    let expr = parser.parse_or()?;
    if parser.index != tokens.len() {
        bail!("unexpected trailing tokens");
    }
    Ok(expr)
}

pub fn eval_expression(input: &str, context: &HashMap<String, Value>) -> Result<bool> {
    let expr = parse_expression(input)?;
    eval_ast(&expr, context)?.as_bool()
}

pub fn eval_ast(expr: &Expr, context: &HashMap<String, Value>) -> Result<Value> {
    match expr {
        Expr::Value(value) => Ok(value.clone()),
        Expr::Identifier(name) => Ok(context.get(name).cloned().unwrap_or(Value::None)),
        Expr::FunctionCall { name, args } => {
            let values = args
                .iter()
                .map(|arg| eval_ast(arg, context))
                .collect::<Result<Vec<_>>>()?;
            match (name.as_str(), values.as_slice()) {
                ("match", [Value::Str(pattern), value]) => Ok(Value::Bool(
                    Regex::new(pattern)?.is_match(&value.as_string())
                        && Regex::new(pattern)?
                            .find(&value.as_string())
                            .map(|matched| matched.as_str() == value.as_string())
                            .unwrap_or(false),
                )),
                ("search", [Value::Str(pattern), value]) => Ok(Value::Bool(
                    Regex::new(pattern)?.is_match(&value.as_string()),
                )),
                _ => bail!("unsupported function call"),
            }
        }
        Expr::Compare { left, op, right } => {
            let left_value = eval_ast(left, context)?;
            let right_value = eval_ast(right, context)?;
            Ok(Value::Bool(compare_values(&left_value, *op, &right_value)?))
        }
        Expr::And(left, right) => Ok(Value::Bool(
            eval_ast(left, context)?.as_bool()? && eval_ast(right, context)?.as_bool()?,
        )),
        Expr::Or(left, right) => Ok(Value::Bool(
            eval_ast(left, context)?.as_bool()? || eval_ast(right, context)?.as_bool()?,
        )),
    }
}

fn compare_values(left: &Value, op: CompareOp, right: &Value) -> Result<bool> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Ok(match op {
            CompareOp::Eq => left == right,
            CompareOp::Ne => left != right,
            CompareOp::Gt => left > right,
            CompareOp::Ge => left >= right,
            CompareOp::Lt => left < right,
            CompareOp::Le => left <= right,
        }),
        (Value::Str(left), Value::Str(right)) => Ok(match op {
            CompareOp::Eq => left == right,
            CompareOp::Ne => left != right,
            CompareOp::Gt => left > right,
            CompareOp::Ge => left >= right,
            CompareOp::Lt => left < right,
            CompareOp::Le => left <= right,
        }),
        (Value::Bool(left), Value::Bool(right)) => Ok(match op {
            CompareOp::Eq => left == right,
            CompareOp::Ne => left != right,
            _ => bail!("unsupported boolean comparison"),
        }),
        (Value::None, Value::None) => {
            Ok(matches!(op, CompareOp::Eq | CompareOp::Ge | CompareOp::Le))
        }
        (Value::None, _) | (_, Value::None) => Ok(matches!(op, CompareOp::Ne)),
        _ => bail!("unsupported comparison"),
    }
}

fn tokenize(input: &str) -> Result<Vec<Token>> {
    let mut chars = input.chars().peekable();
    let mut tokens = Vec::new();

    while let Some(ch) = chars.peek().copied() {
        match ch {
            ' ' | '\t' | '\r' | '\n' => {
                chars.next();
            }
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RParen);
            }
            ',' => {
                chars.next();
                tokens.push(Token::Comma);
            }
            '=' => {
                chars.next();
                if chars.next() != Some('=') {
                    bail!("expected ==");
                }
                tokens.push(Token::Eq);
            }
            '!' => {
                chars.next();
                if chars.next() != Some('=') {
                    bail!("expected !=");
                }
                tokens.push(Token::Ne);
            }
            '>' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    tokens.push(Token::Ge);
                } else {
                    tokens.push(Token::Gt);
                }
            }
            '<' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    tokens.push(Token::Le);
                } else {
                    tokens.push(Token::Lt);
                }
            }
            '"' => {
                chars.next();
                let mut value = String::new();
                while let Some(current) = chars.next() {
                    match current {
                        '\\' => {
                            let escaped =
                                chars.next().ok_or_else(|| anyhow!("unterminated escape"))?;
                            value.push(escaped);
                        }
                        '"' => break,
                        other => value.push(other),
                    }
                }
                tokens.push(Token::String(value));
            }
            '-' | '0'..='9' => {
                let mut value = String::new();
                while let Some(current @ ('-' | '0'..='9')) = chars.peek().copied() {
                    value.push(current);
                    chars.next();
                }
                tokens.push(Token::Number(value.parse()?));
            }
            '_' | 'a'..='z' | 'A'..='Z' => {
                let mut ident = String::new();
                while let Some(current @ ('_' | 'a'..='z' | 'A'..='Z' | '0'..='9')) =
                    chars.peek().copied()
                {
                    ident.push(current);
                    chars.next();
                }
                match ident.as_str() {
                    "and" => tokens.push(Token::And),
                    "or" => tokens.push(Token::Or),
                    "true" => tokens.push(Token::String("true".to_string())),
                    "false" => tokens.push(Token::String("false".to_string())),
                    _ => tokens.push(Token::Identifier(ident)),
                }
            }
            other => bail!("unexpected token {other:?}"),
        }
    }

    Ok(tokens)
}

struct Parser<'a> {
    tokens: &'a [Token],
    index: usize,
}

impl<'a> Parser<'a> {
    fn parse_or(&mut self) -> Result<Expr> {
        let mut expr = self.parse_and()?;
        while self.try_consume(&Token::Or) {
            let right = self.parse_and()?;
            expr = Expr::Or(Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut expr = self.parse_compare()?;
        while self.try_consume(&Token::And) {
            let right = self.parse_compare()?;
            expr = Expr::And(Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_compare(&mut self) -> Result<Expr> {
        let left = self.parse_primary()?;
        let op = match self.peek() {
            Some(Token::Eq) => Some(CompareOp::Eq),
            Some(Token::Ne) => Some(CompareOp::Ne),
            Some(Token::Gt) => Some(CompareOp::Gt),
            Some(Token::Ge) => Some(CompareOp::Ge),
            Some(Token::Lt) => Some(CompareOp::Lt),
            Some(Token::Le) => Some(CompareOp::Le),
            _ => None,
        };

        if let Some(op) = op {
            self.index += 1;
            let right = self.parse_primary()?;
            Ok(Expr::Compare {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.next().cloned() {
            Some(Token::Number(value)) => Ok(Expr::Value(Value::Int(value))),
            Some(Token::String(value)) => Ok(Expr::Value(Value::Str(value))),
            Some(Token::Identifier(ident)) => {
                if self.try_consume(&Token::LParen) {
                    let mut args = Vec::new();
                    if !self.try_consume(&Token::RParen) {
                        loop {
                            args.push(self.parse_or()?);
                            if self.try_consume(&Token::RParen) {
                                break;
                            }
                            self.expect(&Token::Comma)?;
                        }
                    }
                    Ok(Expr::FunctionCall { name: ident, args })
                } else {
                    Ok(Expr::Identifier(ident))
                }
            }
            Some(Token::LParen) => {
                let expr = self.parse_or()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            other => bail!("unexpected token {other:?}"),
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn next(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.index);
        if token.is_some() {
            self.index += 1;
        }
        token
    }

    fn try_consume(&mut self, token: &Token) -> bool {
        if self.peek() == Some(token) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, token: &Token) -> Result<()> {
        if self.try_consume(token) {
            Ok(())
        } else {
            bail!("expected token {token:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{eval_expression, parse_expression, Value};
    use std::collections::HashMap;

    #[test]
    fn parses_current_layout_candidate_expression() {
        parse_expression(r#"usage_page == 65440 or (vendor_id == 2816 and product_id == 4097)"#)
            .expect("candidate should parse");
    }

    #[test]
    fn evaluates_search_and_missing_usage_page() {
        let mut context = HashMap::new();
        context.insert("vendor_id".to_string(), Value::Int(2816));
        context.insert("product_id".to_string(), Value::Int(4097));
        assert!(eval_expression(
            r#"usage_page == 65440 or (vendor_id == 2816 and product_id == 4097)"#,
            &context
        )
        .expect("expression should evaluate"));
    }

    #[test]
    fn evaluates_match_expression() {
        let mut context = HashMap::new();
        context.insert(
            "firmware".to_string(),
            Value::Str("V25.MSD_TWO.01.005".into()),
        );
        assert!(
            eval_expression(r#"search("V25\\.MSD_TWO|MSD_TWO", firmware)"#, &context)
                .expect("expression should evaluate")
        );
    }
}
