<?php //echo $_GET['typeahead'];
require 'connection.php';
$query = null;
if(isset($_GET['typeahead']))
{
    $key = $_GET['typeahead'];
    $array = array();

    $query = $db->query("select * from restaurant where name LIKE '{$key}%'");

}
?>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Ratatouille: What’s Actually Cooking in the Kitchen</title>
    <meta content="IE=edge" http-equiv="X-UA-Compatible">
    <meta content="width=device-width, initial-scale=1" name="viewport">
    <meta content="Bodo - Simple One Page Personal" name="description">
    <meta content="BdgPixel" name="author">
    <!--Fav-->
    <link href="images/favicon.ico" rel="shortcut icon">
    
    <!--styles-->
    <link href="css/bootstrap.min.css" rel="stylesheet">
    <link href="css/owl.carousel.css" rel="stylesheet">
    <link href="css/owl.theme.css" rel="stylesheet">
    <link href="css/magnific-popup.css" rel="stylesheet">
    <link href="css/style.css" rel="stylesheet">
    <link href="css/responsive.css" rel="stylesheet">
    
    <!--fonts google-->
    <link href='https://fonts.googleapis.com/css?family=Roboto+Slab:400,700' rel='stylesheet' type='text/css'>
    <link href='https://fonts.googleapis.com/css?family=Roboto:400,300,500,700' rel='stylesheet' type='text/css'>
    
    <!--[if lt IE 9]>
       <script type="text/javascript" src="js/html5shiv.min.js"></script>
    <![endif]-->
	<style type="text/css">
.bs-example{
	font-family: sans-serif;
	position: relative;
	margin: 50px;
}
.typeahead, .tt-query, .tt-hint {
	border: 2px solid #CCCCCC;
	border-radius: 8px;
	font-size: 24px;
	height: 30px;
	line-height: 30px;
	outline: medium none;
	padding: 8px 12px;
	width: 396px;
}
.typeahead {
	background-color: #FFFFFF;
}
.typeahead:focus {
	border: 2px solid #0097CF;
}
.tt-query {
	box-shadow: 0 1px 1px rgba(0, 0, 0, 0.075) inset;
	height:33;
	width:400; 
}
.tt-hint {
	color: #999999;
}
.tt-dropdown-menu {
	background-color: #FFFFFF;
	border: 1px solid rgba(0, 0, 0, 0.2);
	border-radius: 8px;
	box-shadow: 0 5px 10px rgba(0, 0, 0, 0.2);
	margin-top: 12px;
	padding: 8px 0;
	width: 422px;
}
.tt-suggestion {
	font-size: 24px;
	line-height: 24px;
	padding: 3px 20px;
}
.tt-suggestion.tt-is-under-cursor {
	background-color: #0097CF;
	color: #FFFFFF;
}
.tt-suggestion p {
	margin: 0;
}
</style>


 <script src="http://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css">
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>
    <script src="typeahead.min.js"></script>
    <script>
    $(document).ready(function(){
    $('input.typeahead').typeahead({
        name: 'typeahead',
        remote:'search.php?key=%QUERY',
        limit : 10
    });
});
    </script>


  </head>
  <body>
    <!--PRELOADER-->
    <div id="preloader">
      <div id="status">
        <img alt="logo" src="images/logo-big.png">
      </div>
    </div>
    <!--/.PRELOADER END-->

    <!--HEADER -->
    <div class="header">
      <div class="for-sticky">
        <!--LOGO-->
        <div class="col-md-2 col-xs-6 logo">
          <a href="index.html"><img alt="logo" class="logo-nav" src="images/logo.png"></a>
        </div>
        <!--/.LOGO END-->
      </div>
      <div class="menu-wrap">
        <nav class="menu">
          <div class="menu-list">
            <a data-scroll="" href="#home" class="active">
              <span>Home</span>
            </a>
            <a data-scroll="" href="#about">
              <span>Search</span>
            </a>
           
            <a data-scroll="" href="#findings">
              <span>Findings</span>
            </a>
  
            <a data-scroll="" href="#testimonial">
              <span>Thank You</span>
            </a>
			 <a data-scroll="" href="#work">
              <span>ABOUT US</span>
            </a>
          </div>
        </nav>
        <button class="close-button" id="close-button">Close Menu</button>
      </div>
      <button class="menu-button" id="open-button">
        <span></span>
        <span></span>
        <span></span>
      </button><!--/.for-sticky-->
    </div>
    <!--/.HEADER END-->
    
    <!--CONTENT WRAP-->
    <div class="content-wrap">
      <!--CONTENT-->
      <div class="content">
        <!--HOME-->
        <section id="home">
          <div class="container">
            <div class="row">
              <div class="wrap-hero-content">
                  <div class="hero-content">
                    <h1>CMPT 733</h1>
                    <br>
                    <span class="typed"></span>
                  </div>
              </div>
              <div class="mouse-icon margin-20">
                <div class="scroll"></div>
              </div>
            </div>
          </div>
        </section>
        <!--/.HOME END-->

        <!--ABOUT-->
        <section id="about">
	<div class="container" style="background-color:Beige">
	</br> </br> </br> </br>
	<div align="center">
        <h1 align="center">Ratatouille: What’s Actually Cooking in the Kitchen</h1>
		</br></br>
        <div class="row">

            <form action="" method="get">
            <div class="col-md-6 col-md-offset-3">
            <input type="text" name="typeahead" class="typeahead tt-query" autocomplete="off" spellcheck="false" placeholder="<?php if(!isset($_GET['typeahead'])) echo '';else echo $key; ?>">
            </div>
            <div class="col-md-2">
               <button class="btn btn-warning" type="submit" name="submit" href="#about" ><img src = "images/submit.png" height="25" width="150"></></button>
            </div>
            </form>
        </div>
        <br>
        <div>
        <?php
        //var_dump($query);
                if(isset($query) && mysqli_num_rows($query)>0) {
                        echo '<table class="table table-bordered"><tr><th>Name</th><th>Yelp Star</th><th>Average Violations Per Year</th><th>Risk</th></tr>';

                        foreach($query as $row)
                        {
                            echo '<tr><td>'.$row['name'].'</td><td>'.$row['yelp_star'].'</td><td>'.$row['violations'].'</td><td>'.$row['risk'].'</td></tr>';

                        }
                        echo '</table>';
                }

        ?>
		
		<img src = "images/mouse.jpeg" height="400" width = "1000"/>
		</div>
        </div>
    </div>
		 
		 
        </section>
        <!--/.ABOUT END-->
		
        <!--Findings-->
        <section class="grey-bg" id="findings">
          <div class="container">
            <div class="row">
              <div class="col-md-3">
                <h3 class="title-small">
                  <span>Interesting Findings</span>
                </h3>
                <p class="content-detail">
				<h5>
                 Some of the interesting findings of this project
				 </h5>
                </p>
              </div>
              <div class="col-md-9 content-right">
                <div class="row">
                  <ul class="listing-item">
                    <li>
                      <div class="col-md-6 col-sm-6">
                        <div class="wrap-card">
                          <div class="card">
                            <h2 class="year">
                              Do All big restaurant chains behave same ? 
                            </h2>
                            <p class="job">
                              7 - Eleven Food Store
                            </p>
                            <p class="company">
                              In City of Las Vegas
                            </p>
                            <hr>
                            <div class="text-detail">
                              <p>
                              <a href="images/all_vio_7Elevel.png"><img alt="image" src="images/all_vio_7Elevel.png"></a>
                              </p>
                            </div>
                          </div>
                        </div>
                      </div>
                    </li>

                    <li>
                      <div class="col-md-6 col-sm-6">
                        <div class="wrap-card">
                          <div class="card">
                            <h2 class="year">
                              Geo Location
                            </h2>
                            <p class="job">
                              Las Vegas
                            </p>
                            <p class="company">
                             All Restaurant are plotted on map based on their latitude and longitude
                            </p>
                            <hr>
                            <div class="text-detail">
                              <p>
								<a href="images/maps.png"><img alt="image" src="images/maps.png"></a>
                              </p>
                            </div>
                          </div>
                        </div>
                      </div>
                    </li>
                  </ul>  
                  <ul class="listing-item">
                    <li>
                      <div class="col-md-6 col-sm-6">
                        <div class="wrap-card">
                          <div class="card">
                            <h2 class="year">
                              Crowdsourcing
                            </h2>
                            <p class="job">
                              Demo
                            </p>
                            <p class="company">
                              Crowd Flower
                            </p>
                            <hr>
                            <div class="text-detail">
                              <p>
                               <a href="https://tasks.crowdflower.com/channels/cf_internal/jobs/889150/editor_preview"><img alt="image" src="images/crowd.png"></a>
                              </p>
                            </div>
                          </div>
                        </div>
                      </div>
                    </li>

                    <li>
                      <div class="col-md-6 col-sm-6">
                        <div class="wrap-card">
                          <div class="card">
                            <h2 class="year">
                              Learnings
                            </h2>
                            <p class="job">
                              Project
                            </p>
                            <p class="company">
                              CMPT 733
                            </p>
                            <hr>
                            <div class="text-detail">
                              <p>
                                Importance of Entity Resolution                    
                                Advantage and need of Crowd Sourcing
                                Challenges with Crowd Sourcing
                                Challenges we face when working with data from different sources
                                Text semantics analysis								
								</p>
                            </div>
                          </div>
                        </div>
                      </div>
                    </li>
                  </ul>

                </div>
              </div>
            </div>
          </div>
        </section>
      
        <!--TESTIMONIAL-->
        <section id="testimonial">
          <div class="container">
            <div class="row wrap-testimonial">
              <div class="col-md-10 col-md-offset-1">
                <div class="owl-carousel">
                  <div class="list-testimonial">
                    <div class="content-testimonial">
                      <h3 class="testi">
                        “ Food, in the end, in our own tradition, is something holy. It's not about nutrients and calories. It's about sharing. It's about honesty. It's about identity. ”
                      </h3>
                      <p class="people">
                        Louise Fresco 
                      </p>
                    </div>
                  </div>
                  <div class="list-testimonial">
                    <div class="content-testimonial">
                      <h3 class="testi">
                        “ Life is uncertain. Eat dessert first ”
                      </h3>
                      <p class="people">
                        Ernestine Ulmer
                      </p>
                    </div>
                  </div>
                  <div class="list-testimonial">
                    <div class="content-testimonial">
                      <h3 class="testi">
                        “ The secret of success in life is to eat what you like and let the food fight it out inside. ”
                      </h3>
                      <p class="people">
                        Mark Twain
                      </p>
                    </div>
                  </div>
				  <div class="list-testimonial">
                    <div class="content-testimonial">
                      <h2 class="testi">
                        “ Thank you for Visiting our Site. ”
                      </h2>
                      <p class="people">
                        Mangesh Bhangare          Saif Charaniya          Jay Naidu
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="mask-testimonial"></div>
        </section>
        <!--/.TESTMONIAL END-->
        
        <!--WORK-->
        <section class="grey-bg mar-tm-10" id="work">
          <div class="container">
            <div class="row">
              <div class="col-md-3">
                <h3 class="title-small">
                  <span>ABOUT US</span>
                </h3>
                <p class="content-detail">
                  Mangesh Bhangare <br> <a href="mailto:mbhangar@sfu.ca">Send Mail</a>
				  <br><br> Saif Charaniya <br><a href="mailto:saifc@sfu.ca">Send Mail</a>
				  <br><br> Jay Naidu <br><a href="mailto:jna50@sfu.ca">Send Mail</a>
                </p>
              </div>
              <div class="col-md-9 content-right">
                <!--PORTFOLIO IMAGE-->
                <ul class="portfolio-image">
                  <li class="col-md-6">
                    <a href="https://www.linkedin.com/in/mangesh-bhangare-b3a72547"><img alt="image" src="images/mangesh.jpg">
                      <div class="decription-wrap">
                        <div class="image-bg">
                           <p class="desc">Mangesh Bhangare</p>
                        </div>

                      </div>
                    </a>
                  </li>
                  <li class="col-md-6">
                    <a href="images/saif.jpg"><img alt="image" src="images/saif.jpg">
                      <div class="decription-wrap">
                        <div class="image-bg">
                          <p class="desc">
                            Saif Charaniya
                          </p>
                        </div>
                      </div>
                    </a>
                  </li>
                  <li class="col-md-6">
                    <a href="images/jay.jpg"><img alt="image" src="images/jay.jpg">
                      <div class="decription-wrap">
                        <div class="image-bg">
                          <p class="desc">
                            Jay Naidu
                          </p>
                        </div>
                      </div>
                    </a>
                  </li>
                  <li class="col-md-6">
                    <a href="http://sfu.ca/"><img alt="image" src="images/sfu.png">
                      <div class="decription-wrap">
                        <div class="image-bg">
                          <p class="desc">
                            Simon Fraser University
                          </p>
                        </div>
                      </div>
                    </a>
                  </li>
                 
                </ul>
                <!--/.PORTFOLIO IMAGE END-->
              </div>
            </div>
          </div>
        </section>
        <!--/.WORK END-->
		
        <!--FOOTER-->
        <footer>
         <div class="footer-bottom">
            <div class="container">
              <div class="row">
                <a href="http://sfu.ca/"><img src="images/sfu.png" alt="Simon Fraser University" class="center-block" /></a>
			 </div>
            </div>
          </div>
        </footer>
        <!--/.FOOTER-END-->

      <!--/.CONTENT END-->
      </div>
    <!--/.CONTENT-WRAP END-->
    </div>
    

    
   
    <script src="js/bootstrap.min.js" type="text/javascript"></script>
    <script src="js/classie.js" type="text/javascript"></script>
    <script src="js/owl.carousel.min.js" type="text/javascript"></script>
    <script src="js/masonry.pkgd.min.js" type="text/javascript"></script>
    <script src="js/masonry.js" type="text/javascript"></script>
    <script src="js/smooth-scroll.min.js" type="text/javascript"></script>
    <script src="js/typed.js" type="text/javascript"></script>
    <script src="js/main.js" type="text/javascript"></script>
  </body>
</html>