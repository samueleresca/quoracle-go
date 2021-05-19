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

func (exp Or) String() string {
   return fmt.Sprintf("%b", exp.Es)
}

type And struct {
   Es []GenericExpr
}

func (exp *And) String() string {
   return fmt.Sprintf("%b", exp.Es)
}

func (exp *And) Quorums() <- chan  map[GenericExpr]bool {
   chnl := make(chan map[GenericExpr]bool)
   go func() {
      for _, e := range exp.Es {
         chnl <- e.Quorum()
      }

      // Ensure that at the end of the loop we close the channel!
      close(chnl)
   }()
   return chnl
}

func (exp *And) IsQuorum(xs map[GenericExpr]bool) bool {
 var found = false
 for  _, e := range exp.Es {
     if e.IsQuorum(xs) {
        found = true
        return found
     }
   }
   return found
}



