#import "setup/template.typ": *
#include "setup/capa.typ"
#import "setup/sourcerer.typ": code
// #import "@preview/sourcerer:0.2.1": code
#show: project
#counter(page).update(1)
#import "@preview/algo:0.3.3": algo, i, d, comment //https://github.com/platformer/typst-algorithms
#import "@preview/tablex:0.0.8": gridx, tablex, rowspanx, colspanx, vlinex, hlinex, cellx

#import "@preview/codly:1.2.0": *
#import "@preview/codly-languages:0.1.1": *
#show: codly-init.with()
#codly(languages: codly-languages)
#codly(zebra-fill: none)

#set text(lang: "pt", region: "pt")
#show link: underline
#show link: set text(rgb("#004C99"))
#show ref: set text(rgb("#00994C"))
#set heading(numbering: "1.")
#show raw.where(block: false): box.with(
  fill: luma(240),
  inset: (x: 0pt, y: 0pt),
  outset: (y: 3pt),
  radius: 3pt,
)

#import "@preview/oasis-align:0.2.0": *

//#page(numbering:none)[
//  #outline(indent: 2em, depth: 7)  
//  // #outline(target: figure)
//]
#pagebreak()
#counter(page).update(1)

//#show heading.where(level:1): set heading(numbering: (l) => {[Parte #l] })

#set list(marker: ([•], [‣], [–]))


= Descrição do problema <1.Multiclass>

Para este projeto foi utilizada uma base de dados de #link("https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data")[eCommerce], proveniente de uma loja com múltiplas categorias e tipos de produtos. Na nossa análise consideramos os meses de outubro e novembro de 2019, os quais contêm registos detalhados (_logs_) das ações efetuadas pelos utilizadores no _site_ da loja, tais como visualizações de produtos, adições ao carrinho e compras#footnote[Na secção @decisions serão detalhadas todas as assunções tomadas relativamente ao conjunto de dados utilizado]. 

//Por isso, cada _log_/linha do _dataset_ corresponde a alguma destas interações, que estará sempre associado a um _user_, a um produto e a um momento concreto no tempo.

// Na fase da @decisions vamos explorar mais detalhadamente  


O foco principal da nossa análise é identificar possíveis padrões de consumo dos utilizadores, com base nas suas interações no _site_, de forma a agrupá-los segundo preferências e comportamentos semelhantes. Por exemplo, poderá emergir um grupo de clientes $X$ com maior interesse em produtos da categoria $α$ de uma gama mais elevada (produtos com preço superior). Assim, o problema enquadra-se no domínio da *aprendizagem não supervisionada*, sendo abordado através de técnicas de _clustering_.

A componente de _Clustering_ consiste na aplicação de diferentes algoritmos, como #link("https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.clustering.KMeans.html")[K-Means] e #link("https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.clustering.GaussianMixture.html")[Gaussian Mixture Models] (GMM), sobre um conjunto de variáveis selecionadas a partir das interações dos utilizadores. Com esta abordagem, pretendemos responder a duas questões principais:

- Como podemos caracterizar os grupos formados (_clusters_), através das/*da frequência das visualizações e preços dos produtos*/ _features_ a utilizar? É possível observar padrões e grupos coesos?
- Como varia a complexidade temporal dos algoritmos utilizados, especialmente quando aplicados a subconjuntos de dados com dimensões distintas?

//Numa fase seguinte é então desenvolvido um sistema de recomendações simples, como forma de aplicar os resultados do _clustering_ a um contexto prático. Esta componente assume particular importância enquanto forma de dar utilidade do nosso agrupamento, ao permitir a recomendação de produtos a utilizadores com perfis semelhantes.

Numa fase seguinte é então desenvolvido um sistema de recomendações simples com base em similaridade item-item. Embora esta abordagem seja independente do _clustering_, ambas as análises são complementares. Enquanto o _clustering_ ajuda a revelar padrões de consumo e perfis de utilizador, a recomendação foca-se em associar/"agrupar" produtos semelhantes aos já visualizados ou comprados, a partir da co-ocorrência entre itens.

#line(length: 100%)

Para a entrega final ponderamos falar dos seguintes tópicos em cada uma das fases:

= Preparação de dados<decisions>

- Explicar o joining dos dados e formato a utilizar (parquet);
- Perceber o porquê de utilizarmos as "views" para o _Clustering_;
- Divisão em categorias e sub-categorias;
- Pivot table para o propósito do problema;
- (...).

= Experiências e testes realizados

- Inicialmente para 10000 dados apenas para fins de testes;
- Incrementação de dados e testagem para o "_dataset full_";
- Tentar imaginar qual seria o tempo computacional se o conjunto de dados fosse 400% do disponível, por exemplo (curva de análise temporal);
- (...).

= Resultados

- Comentar a formação dos _clusters_;
- Interpretar a análise de complexidade.

Link do GitHub com o desenvolvimento do projeto: 


#pagebreak()
#set heading(numbering: none)
#show heading.where(level:1): set heading(numbering: none)

= Anexos <Anexos> // IF NEEDED
#set heading(numbering: (level1, level2,..levels ) => {
  if (levels.pos().len() > 0) {
    return []
  }
  ("Anexo", str.from-unicode(level2 + 64)/*, "-"*/).join(" ")
}) // seria so usar counter(heading).display("I") se nao tivesse o resto
//show heading(level:3)

== - (...) <teste>


/*#figure(
image("images/lrate_01.png", width: 85%),
  caption: [Modelo de classificação multi-classe com valor de *0.01* para o `learning_rate`]
)*/
