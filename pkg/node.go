package pkg

import "fmt"

type GenericExpr interface {
   Add(expr GenericExpr) GenericExpr
   Multiply(expr GenericExpr) GenericExpr
   String() string
}


type Expr struct {

}

func (lhs Expr) Add(rhs GenericExpr) GenericExpr {
   return lhs.Multiply(rhs)
}

func (lhs Expr) Multiply(rhs GenericExpr) GenericExpr {
   return lhs.Multiply(rhs)
}

type Or struct {
   Es []GenericExpr
}

func (exp Or) String() string {
   return fmt.Sprintf("%b", exp.Es)
}



type And struct {
   Es []GenericExpr
}

func (exp And) String() string {
   return fmt.Sprintf("%b", exp.Es)
}