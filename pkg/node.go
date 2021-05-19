package pkg

import "fmt"


type Node struct {

}

type GenericExpr interface {
   Add(expr GenericExpr) GenericExpr
   Multiply(expr GenericExpr) GenericExpr
   Quorum() map[GenericExpr]bool
   IsQuorum(map[GenericExpr]bool) bool
   Nodes() map[Node]bool
   NumLeaves() int
   DupFreeMinFailures() int
   String() string
}


type Expr struct {


}

func (lhs Expr) Add(rhs GenericExpr) GenericExpr {
   return lhs.Add(rhs)
}

func (lhs Expr) Multiply(rhs GenericExpr) GenericExpr {
   return lhs.Multiply(rhs)
}




type Or struct {
   Es []GenericExpr
}

func (exp Or) NumLeaves() int {
    panic("implement me")
}

func (exp Or) DupFreeMinFailures() int {
    panic("implement me")
}

func (exp Or) Add(expr GenericExpr) GenericExpr {
    panic("implement me")
}

func (exp Or) Multiply(expr GenericExpr) GenericExpr {
    panic("implement me")
}

func (exp Or) Quorum() map[GenericExpr]bool {
    panic("implement me")
}

func (exp Or) IsQuorum(m map[GenericExpr]bool) bool {
    panic("implement me")
}

func (exp Or) Nodes() map[Node]bool {
    panic("implement me")
}

func (exp Or) String() string {
   return fmt.Sprintf("%b", exp.Es)
}

type And struct {
   Es []GenericExpr
}

func (expr *And) String() string {
   return fmt.Sprintf("%b", expr.Es)
}

func (expr *And) Quorums() <- chan  map[GenericExpr]bool {
   chnl := make(chan map[GenericExpr]bool)
   go func() {
      for _, e := range expr.Es {
         chnl <- e.Quorum()
      }

      // Ensure that at the end of the loop we close the channel!
      close(chnl)
   }()
   return chnl
}

func (expr *And) IsQuorum(xs map[GenericExpr]bool) bool {
 var found = false
 for  _, e := range expr.Es {
     if e.IsQuorum(xs) {
        found = true
        return found
     }
   }
   return found
}


func (expr *And) Nodes() map[Node]bool {
    var final = make(map[Node]bool)

    for _, e := range expr.Es {
        for n := range e.Nodes() {
            final[n] = true
        }
    }
    return final
}

func (expr *And) Dual() GenericExpr {
    return Or{expr.Es}
}


func (expr *And) NumLeaves() int {
    total := 0

    for _, e := range expr.Es {
        total += e.NumLeaves()
    }

    return total
}

func (expr *And) DupFreeMinFailures() int {
    var exprs = expr.Es
    var min int = exprs[0].DupFreeMinFailures()

    for _, expr := range exprs {
        if min > expr.DupFreeMinFailures() {
            min = expr.DupFreeMinFailures()
        }
    }
    return min
}



