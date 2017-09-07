/*
Created by: Andrew Minard
Date created: Sept 6, 2017
Description: Browser macro to automate unfollow multiple accounts, except those that are specific in the whitelist.
Instructions: 
	1. Login to your Instagram account
	2. View your Profile 
	3. Click on "Following" to view the accounts you follow
	4. Play this .js script in the iMacro extension (for Firefox)
Options: 
	1. Populate the whiteList variable with accounts you want to keep following 
	2. Adjust wait time after each unfollow
	3. May need to adjust the TAG class to match what IG is using at the time
*/

// Populate this array with the accounts you want to keep following
var whiteList = ["user_name1",
				"user_name2",
				"user_name3"
				];

var init;
init = "CODE:";
init += "SET !TIMEOUT_STEP 1"+"\n";
init += "SET !ERRORIGNORE YES"+"\n";
//init += "TAB T=1"+"\n";
//init += "TAB CLOSEALLOTHERS"+"\n";
//init += "SET !EXTRACT NULL"+"\n";
//init += "TAG POS=1 TYPE=A ATTR=CLASS:_2g7d5<SP>notranslate<SP>_o5iw8&&TXT:LASTUSERHERE EXTRACT=TITLE"+"\n";
//init += "SET !ENDOFPAGE {{!TAGSOURCEINDEX}}"+"\n";

var getUser;
getUser = "CODE:";
getUser += "TAG POS={{userPos}} TYPE=A ATTR=CLASS:_2g7d5<SP>notranslate<SP>_o5iw8&&TXT:* EXTRACT=TITLE"+"\n";

var checkButton;
checkButton = "CODE:";
checkButton += "TAG POS={{userPos}} TYPE=A ATTR=CLASS:_2g7d5<SP>notranslate<SP>_o5iw8&&TXT:* EXTRACT=TITLE"+"\n";
checkButton += "SET !EXTRACT NULL"+"\n";
checkButton += "TAG POS=R1 TYPE=BUTTON ATTR=TXT:Follow* EXTRACT=TXT"+"\n";

var unfollow;
unfollow = "CODE:";
unfollow += "TAG POS=1 TYPE=A ATTR=CLASS:_2g7d5<SP>notranslate<SP>_o5iw8&&TXT:{{userName}}* EXTRACT=TITLE"+"\n";
unfollow += "TAG POS=R1 TYPE=BUTTON ATTR=TXT:Following"+"\n";
unfollow += "WAIT SECONDS=15"+"\n";

iimPlay(init);
//var errors = 0;
for (i=1; i<=1000; i++) {
	//alert("userPos: "+i);
	// Get next username
	iimSet("userPos", i);
	iimPlay(getUser);
	var user = iimGetLastExtract();
	// Check button text next to each user to determine if we already unfollowed them
	iimSet("userPos", i);
	iimPlay(checkButton);
	var buttonTxt = iimGetLastExtract();
	//alert(user+", "+buttonTxt);
	// Check for absence in whitelist, and verify that we follow them now. Then unfollow.
	if (whiteList.indexOf(user) == -1 && buttonTxt == "Following") {
		iimSet("userName", user);
		var code = iimPlay(unfollow);
		/*if (code != 1){
			errors++;
			var error = iimGetLastError();
			alert("ERROR:  "+code+"\n\n"+error);
		}*/
	}
}
//alert("Errors occurred: "+errors);
